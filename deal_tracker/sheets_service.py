# deal_tracker/sheets_service.py
import gspread
import gspread.exceptions
from oauth2client.service_account import ServiceAccountCredentials  # type: ignore
import logging
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import datetime
from datetime import timezone, timedelta
import re
import time

import config

logger = logging.getLogger(__name__)

# --- КЭШИРОВАНИЕ: ---
# Глобальный кэш для хранения данных, прочитанных из таблиц
_data_cache: dict[str, tuple[list[dict], float]] = {}
# Время жизни кэша в секундах
CACHE_DURATION_SECONDS = 5
# --------------------

_cached_headers = {}

# --- СЛОВАРИ СООТВЕТСТВИЯ ПОЛЕЙ (FIELD MAPS) ---
CORE_TRADES_FIELD_MAP = {
    'Timestamp': ['Timestamp', 'Время сделки', 'Дата операции', 'Время'],
    'Order_ID': ['Order_ID', 'ID ордера', 'Номер заявки'],
    'Exchange': ['Exchange', 'Биржа'],
    'Symbol': ['Symbol', 'Тикер', 'Инструмент', 'Торговая Пара'],
    'Type': ['Type', 'Тип сделки', 'Направление', 'Тип'],
    'Amount': ['Amount', 'Количество', 'Объем'],
    'Price': ['Price', 'Цена'],
    'Total_Quote_Amount': ['Total_Quote_Amount', 'Объем в валюте котировки', 'Объем (Квота)'],
    'TP1': ['TP1', 'Тейк Профит 1'],
    'TP2': ['TP2', 'Тейк Профит 2'],
    'TP3': ['TP3', 'Тейк Профит 3'],
    'SL': ['SL', 'Стоп Лосс'],
    'Risk_USD': ['Risk_USD', 'Риск USD'],
    'Strategy': ['Strategy', 'Стратегия'],
    'Trade_PNL': ['Trade_PNL', 'PNL по сделке'],
    'Commission': ['Commission', 'Комиссия'],
    'Commission_Asset': ['Commission_Asset', 'Валюта комиссии', 'Fee Asset'],
    'Source': ['Source', 'Источник'],
    'Asset_Type': ['Asset_Type', 'Тип актива'],
    'Notes': ['Notes', 'Заметки', 'Примечание'],
    'Fifo_Consumed_Qty': ['Fifo_Consumed_Qty', 'FIFO Потреблено'],
    'Fifo_Sell_Processed': ['Fifo_Sell_Processed', 'FIFO Продажа Обработана'],
    'Trade_ID': ['Trade_ID', 'ID Сделки']
}

FUND_MOVEMENTS_FIELD_MAP = {
    'Movement_ID': ['Movement_ID', 'ID Движения'],
    'Timestamp': ['Timestamp', 'Время операции', 'Дата', 'Время'],
    'Type': ['Type', 'Тип операции', 'Тип'],
    'Asset': ['Asset', 'Актив', 'Валюта'],
    'Amount': ['Amount', 'Сумма', 'Количество'],
    'Source_Entity_Type': ['Source_Entity_Type', 'Тип источника'],
    'Source_Name': ['Source_Name', 'Источник', 'Имя Источника'],
    'Destination_Entity_Type': ['Destination_Entity_Type', 'Тип назначения'],
    'Destination_Name': ['Destination_Name', 'Назначение', 'Имя Назначения'],
    'Fee_Amount': ['Fee_Amount', 'Сумма комиссии', 'Комиссия Сумма'],
    'Fee_Asset': ['Fee_Asset', 'Валюта комиссии', 'Комиссия Актив'],
    'Transaction_ID_Blockchain': ['Transaction_ID_Blockchain', 'TX ID', 'Хэш транзакции', 'TX_ID'],
    'Notes': ['Notes', 'Заметки', 'Описание', 'Примечание']
}

OPEN_POSITIONS_FIELD_MAP = {
    'Symbol': ['Symbol', 'Символ', 'Тикер', 'Инструмент'],
    'Exchange': ['Exchange', 'Биржа'],
    'Net_Amount': ['Net_Amount', 'Amount', 'Количество', 'Объем', 'Кол-во'],
    'Avg_Entry_Price': ['Avg_Entry_Price', 'Avg Price', 'Средняя цена входа', 'Ср. Вход'],
    'Current_Price': ['Current_Price', 'Текущая цена', 'Рыночная цена', 'Тек. Цена'],
    'Unrealized_PNL': ['Unrealized_PNL', 'Unreal PNL', 'Нереализованный PNL', 'Бумажный PNL', 'PNL Сумма'],
    'Last_Updated': ['Last_Updated', 'Последнее обновление']
}

ACCOUNT_BALANCES_FIELD_MAP = {
    'Account_Name': ['Account_Name', 'Счет', 'Имя счета'],
    'Asset': ['Asset', 'Актив', 'Валюта'],
    'Balance': ['Balance', 'Баланс', 'Количество'],
    'Entity_Type': ['Entity_Type', 'Тип счета'],
    'Last_Updated_Timestamp': ['Last_Updated_Timestamp', 'Последнее обновление', 'Время обновления']
}

FIFO_LOG_FIELD_MAP = {
    'Symbol': ['Symbol', 'Символ', 'Тикер'],
    'Buy_Trade_ID': ['Buy_Trade_ID', 'ID Покупки (FIFO)', 'ID Покупки'],
    'Sell_Trade_ID': ['Sell_Trade_ID', 'ID Продажи (FIFO)', 'ID Продажи'],
    'Matched_Qty': ['Matched_Qty', 'Сопоставленное Кол-во', 'Кол-во'],
    'Buy_Price': ['Buy_Price', 'Цена Покупки (FIFO)', 'Цена Покупки'],
    'Sell_Price': ['Sell_Price', 'Цена Продажи (FIFO)', 'Цена Продажи'],
    'Fifo_PNL': ['Fifo_PNL', 'PNL FIFO'],
    'Timestamp_Closed': ['Timestamp_Closed', 'Время Закрытия (FIFO)', 'Время Закрытия'],
    'Buy_Timestamp': ['Buy_Timestamp', 'Время Покупки (исх.)', 'Время Покупки'],
    'Exchange': ['Exchange', 'Биржа']
}

ANALYTICS_FIELD_MAP = {
    'Date_Generated': ['Date_Generated', 'Дата генерации', 'Дата'],
    'Total_Realized_PNL': ['Total_Realized_PNL', 'Общий Реализованный PNL'],
    'Total_Unrealized_PNL': ['Total_Unrealized_PNL', 'Общий Нереализованный PNL'],
    'Net_Total_PNL': ['Net_Total_PNL', 'Чистый Общий PNL'],
    'Total_Trades_Closed': ['Total_Trades_Closed', 'Всего Закрыто Сделок'],
    'Winning_Trades_Closed': ['Winning_Trades_Closed', 'Прибыльных Сделок'],
    'Losing_Trades_Closed': ['Losing_Trades_Closed', 'Убыточных Сделок'],
    'Win_Rate_Percent': ['Win_Rate_Percent', 'Процент Прибыльных Сделок', 'Win Rate'],
    'Average_Win_Amount': ['Average_Win_Amount', 'Средняя Прибыль'],
    'Average_Loss_Amount': ['Average_Loss_Amount', 'Средний Убыток'],
    'Profit_Factor': ['Profit_Factor', 'Профит Фактор'],
    'Expectancy': ['Expectancy', 'Мат. Ожидание'],
    'Total_Commissions_Paid': ['Total_Commissions_Paid', 'Всего Комиссий Уплачено'],
    'Net_Invested_Funds': ['Net_Invested_Funds', 'Чистые Вложения'],
    'Portfolio_Current_Value': ['Portfolio_Current_Value', 'Текущая Стоимость Портфеля'],
    'Total_Equity': ['Total_Equity', 'Общий Капитал'],
    'Notes_Analytics': ['Notes_Analytics', 'Заметки по Аналитике']
}


def invalidate_header_cache(sheet_name: str | None = None):
    global _cached_headers
    if sheet_name:
        if sheet_name in _cached_headers:
            del _cached_headers[sheet_name]
            logger.info(f"Кэш заголовков для '{sheet_name}' очищен.")
    else:
        _cached_headers = {}
        logger.info("Весь кэш заголовков очищен.")


def _get_client():
    try:
        scope = ["https://spreadsheets.google.com/feeds",
                 "https://www.googleapis.com/auth/drive"]
        if not config.GOOGLE_CREDS_JSON_PATH or config.GOOGLE_CREDS_JSON_PATH == 'ВАШ_ПУТЬ_К_JSON_КРЕДЕНШИАЛАМ_GOOGLE':
            logger.error(
                f"GOOGLE_CREDS_JSON_PATH не настроен: '{config.GOOGLE_CREDS_JSON_PATH}'")
            raise ValueError("GOOGLE_CREDS_JSON_PATH не задан.")
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            config.GOOGLE_CREDS_JSON_PATH, scope)
        return gspread.authorize(creds)
    except FileNotFoundError:
        logger.error(
            f"Файл учетных данных Google не найден по пути: {config.GOOGLE_CREDS_JSON_PATH}", exc_info=True)
        raise
    except gspread.exceptions.GSpreadException as e_gs:
        logger.error(
            f"Ошибка gspread при авторизации Google Sheets: {e_gs}", exc_info=True)
        raise
    except Exception as e:
        logger.error(
            f"Непредвиденная ошибка авторизации Google Sheets: {e}", exc_info=True)
        raise


def format_value_for_sheet(value) -> str:
    if isinstance(value, Decimal):
        return str(value).replace('.', ',')
    if isinstance(value, float):
        return str(value).replace('.', ',')
    if value is None:
        return ""
    return str(value)


def _parse_numeric_value(value_str: str | None, default_if_empty: str | None = '0') -> str | None:
    if value_str is None or not str(value_str).strip():
        return default_if_empty
    cleaned_val = str(value_str).replace(',', '.').strip()
    try:
        Decimal(cleaned_val)
        return cleaned_val
    except InvalidOperation:
        logger.debug(
            f"Не удалось распарсить '{value_str}' как число. Default: {default_if_empty}")
        return default_if_empty


def _convert_excel_serial_date_to_string(serial_date_val) -> str | None:
    try:
        if isinstance(serial_date_val, str):
            if not re.fullmatch(r"[\d.,]+", serial_date_val.strip()):
                return None
            serial_date_val = serial_date_val.replace(',', '.')
        serial_date_float = float(serial_date_val)
        if not (1 <= serial_date_float < 200000):
            return None
        base_date = datetime.datetime(1899, 12, 30)
        delta = datetime.timedelta(days=serial_date_float)
        dt_object = base_date + delta
        return dt_object.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None


def get_sheet_by_name(sheet_name: str) -> gspread.Worksheet | None:
    try:
        client = _get_client()
        if not config.SPREADSHEET_ID or config.SPREADSHEET_ID == 'ВАШ_SPREADSHEET_ID':
            logger.error(
                f"SPREADSHEET_ID не настроен: '{config.SPREADSHEET_ID}'")
            raise ValueError("SPREADSHEET_ID не задан.")
        spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
        logger.info(
            f"Успешно открыта таблица: '{spreadsheet.title}' (ID: {config.SPREADSHEET_ID})")
        return spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.SpreadsheetNotFound as e_snf:
        logger.error(
            f"Таблица Google Sheets не найдена. SPREADSHEET_ID: '{config.SPREADSHEET_ID}'. Ошибка: {e_snf}", exc_info=True)
    except gspread.exceptions.WorksheetNotFound as e_wnf:
        logger.error(
            f"Лист '{sheet_name}' не найден в таблице ID '{config.SPREADSHEET_ID}'. Ошибка: {e_wnf}", exc_info=True)
    except gspread.exceptions.APIError as e_api:
        logger.error(
            f"Ошибка API Google Sheets при получении листа '{sheet_name}': {e_api}", exc_info=True)
    except Exception as e:
        logger.error(
            f"Непредвиденная ошибка при получении листа '{sheet_name}': {e}", exc_info=True)
    return None


def get_open_positions_sheet(): return get_sheet_by_name(
    config.OPEN_POSITIONS_SHEET_NAME)


def get_core_trades_sheet(): return get_sheet_by_name(config.CORE_TRADES_SHEET_NAME)
def get_analytics_sheet(): return get_sheet_by_name(config.ANALYTICS_SHEET_NAME)
def get_fifo_log_sheet(): return get_sheet_by_name(config.FIFO_LOG_SHEET_NAME)


def get_system_status_sheet(): return get_sheet_by_name(
    config.SYSTEM_STATUS_SHEET_NAME)


def get_fund_movements_sheet(): return get_sheet_by_name(
    config.FUND_MOVEMENTS_SHEET_NAME)


def get_account_balances_sheet(): return get_sheet_by_name(
    config.ACCOUNT_BALANCES_SHEET_NAME)


def get_headers(sheet_name: str) -> list[str]:
    global _cached_headers
    if sheet_name in _cached_headers:
        logger.debug(f"Исп. кэш заголовков для '{sheet_name}'.")
        return _cached_headers[sheet_name]
    try:
        sheet = get_sheet_by_name(sheet_name)
        if sheet:
            headers = sheet.row_values(1)
            if not headers:
                logger.warning(
                    f"Заголовки в '{sheet_name}' пусты или не найдены.")
                _cached_headers[sheet_name] = []
                return []
            cleaned_headers = [str(h).strip() for h in headers]
            _cached_headers[sheet_name] = cleaned_headers
            return cleaned_headers
        return []
    except gspread.exceptions.APIError as e_api:
        logger.error(
            f"Ошибка API Google Sheets при получении заголовков для '{sheet_name}': {e_api}", exc_info=True)
    except Exception as e:
        logger.error(
            f"Непредвиденная ошибка при получении заголовков для '{sheet_name}': {e}", exc_info=True)
    return []


def append_to_sheet(sheet_name: str, data_row_list: list) -> bool:
    context_log = f"в лист '{sheet_name}', количество элементов: {len(data_row_list)}"
    try:
        sheet = get_sheet_by_name(sheet_name)
        if not sheet:
            logger.error(
                f"Не удалось добавить данные {context_log} - лист не найден.")
            return False
        processed_row = [format_value_for_sheet(
            item) for item in data_row_list]
        sheet.append_row(processed_row, value_input_option='USER_ENTERED')
        logger.info(
            f"Данные успешно добавлены {context_log}. Запись: {processed_row}")
        return True
    except gspread.exceptions.APIError as e_api:
        logger.error(
            f"Ошибка API Google Sheets при добавлении данных {context_log}. Запись: {data_row_list}. Ошибка: {e_api}", exc_info=True)
    except Exception as e:
        logger.error(
            f"Непредвиденная ошибка при добавлении данных {context_log}. Запись: {data_row_list}. Ошибка: {e}", exc_info=True)
    return False


def _find_col_index(header_row_processed_lower: list[str], target_names_processed_lower: list[str]) -> int | None:
    for target_name in target_names_processed_lower:
        try:
            return header_row_processed_lower.index(target_name)
        except ValueError:
            continue
    return None


def batch_get_sheets_data(sheet_names: list[str]) -> dict[str, list[list[str]] | None]:
    """Загружает данные с нескольких листов за один API вызов."""
    results = {name: None for name in sheet_names}
    if not sheet_names:
        return results
    try:
        client = _get_client()
        spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
        logger.info(
            f"Пакетная загрузка листов: {sheet_names} из таблицы: '{spreadsheet.title}'")

        all_sheets_data = spreadsheet.values_batch_get(sheet_names)

        value_ranges = all_sheets_data.get('valueRanges', [])
        for i, value_range in enumerate(value_ranges):
            if i < len(sheet_names):
                sheet_name = sheet_names[i]
                results[sheet_name] = value_range.get('values', [])
            else:
                logger.warning(
                    f"Получены данные без соответствующего имени листа в пакетном запросе: {value_range}")

        return results
    except gspread.exceptions.APIError as e_api:
        logger.error(
            f"Ошибка API Google Sheets при пакетной загрузке листов. Ошибка: {e_api}", exc_info=True)
    except Exception as e:
        logger.error(
            f"Непредвиденная ошибка при пакетной загрузке листов: {e}", exc_info=True)
    return results


def get_all_data_from_sheet(sheet_name: str, field_map: dict, **kwargs) -> list[dict]:
    # --- КЭШИРОВАНИЕ: Проверка кэша перед запросом к API ---
    now = time.time()
    if sheet_name in _data_cache:
        cached_data, timestamp = _data_cache[sheet_name]
        if now - timestamp < CACHE_DURATION_SECONDS:
            logger.info(
                f"Возврат кэшированных данных для листа '{sheet_name}'.")
            return cached_data
    # ---------------------------------------------------------

    try:
        sheet = get_sheet_by_name(sheet_name)
        if not sheet:
            return []

        all_sheet_values = sheet.get_all_values()
        num_rows = len(all_sheet_values)
        logger.info(f"Считано {num_rows} строк(и) из листа '{sheet_name}'.")

        if not all_sheet_values or len(all_sheet_values) < 1:
            # --- КЭШИРОВАНИЕ: Сохраняем пустой результат ---
            _data_cache[sheet_name] = ([], now)
            # -----------------------------------------------
            return []

        headers_from_sheet_cleaned = [str(h).strip()
                                      for h in all_sheet_values[0]]
        headers_from_sheet_lower = [h.lower()
                                    for h in headers_from_sheet_cleaned]
        col_indices: dict[str, int] = {}
        for canonical_name, possible_sheet_names_list in field_map.items():
            possible_sheet_names_processed_lower = [
                psn.lower().strip() for psn in possible_sheet_names_list]
            idx = _find_col_index(headers_from_sheet_lower,
                                  possible_sheet_names_processed_lower)
            if idx is not None:
                col_indices[canonical_name] = idx
            else:
                logger.debug(
                    f"СТОЛБЕЦ НЕ СОПОСТАВЛЕН: '{canonical_name}' не найден в '{sheet_name}'.")
        processed_records = []
        for i, row_values_list_raw in enumerate(all_sheet_values[1:], start=2):
            row_values_list = [str(cell_val).strip()
                               for cell_val in row_values_list_raw]
            if not any(cell_val for cell_val in row_values_list):
                continue
            current_row_data = {cn: row_values_list[idx] if idx < len(
                row_values_list) else "" for cn, idx in col_indices.items()}
            for canonical_name_fm in field_map.keys():
                if canonical_name_fm not in current_row_data:
                    current_row_data[canonical_name_fm] = None
            current_row_data['row_number'] = i
            processed_records.append(current_row_data)

        logger.info(
            f"Обработано и получено {len(processed_records)} записей из '{sheet_name}'.")

        # --- КЭШИРОВАНИЕ: Сохраняем свежие данные в кэш ---
        _data_cache[sheet_name] = (processed_records, now)
        # ----------------------------------------------------

        return processed_records
    except Exception as e:
        logger.error(
            f"Критическая ошибка при получении всех данных из '{sheet_name}': {e}", exc_info=True)
    return []


def get_all_core_trades(): return get_all_data_from_sheet(
    config.CORE_TRADES_SHEET_NAME, CORE_TRADES_FIELD_MAP)


def get_all_open_positions(): return get_all_data_from_sheet(
    config.OPEN_POSITIONS_SHEET_NAME, OPEN_POSITIONS_FIELD_MAP)


def get_all_fund_movements(): return get_all_data_from_sheet(
    config.FUND_MOVEMENTS_SHEET_NAME, FUND_MOVEMENTS_FIELD_MAP)


def get_all_fifo_logs(): return get_all_data_from_sheet(
    config.FIFO_LOG_SHEET_NAME, FIFO_LOG_FIELD_MAP)


def get_analytics_history_records(): return get_all_data_from_sheet(
    config.ANALYTICS_SHEET_NAME, ANALYTICS_FIELD_MAP)


def get_all_balances() -> dict[tuple[str, str], dict[str, Decimal | int]]:
    """
    Получает все балансы из листа 'Account_Balances' и возвращает их в виде словаря
    для быстрого доступа, напрямую используя get_all_data_from_sheet.
    """
    balances: dict[tuple[str, str], dict[str, Decimal | int]] = {}
    logger.info("Запрос всех балансов из Account_Balances...")
    try:
        balance_records = get_all_data_from_sheet(
            config.ACCOUNT_BALANCES_SHEET_NAME, ACCOUNT_BALANCES_FIELD_MAP)

        for record in balance_records:
            account = str(record.get('Account_Name', '')).strip().lower()
            asset = str(record.get('Asset', '')).strip().upper()
            balance_str = record.get('Balance', '0')
            row_num = record.get('row_number')

            if account and asset and row_num is not None:
                try:
                    balance_parsed_str = _parse_numeric_value(balance_str, '0')
                    balance_dec = Decimal(
                        balance_parsed_str) if balance_parsed_str is not None else Decimal('0')
                    key = (account, asset)
                    balances[key] = {'balance': balance_dec,
                                     'row_num': int(row_num)}
                except (InvalidOperation, TypeError):
                    logger.warning(
                        f"Не удалось преобразовать баланс {account}/{asset}: '{balance_str}' в строке {row_num}.")

        logger.info(
            f"Загружено {len(balances)} записей балансов из Account_Balances.")
        return balances
    except Exception as e:
        logger.error(
            f"Крит. ошибка при загрузке всех балансов: {e}", exc_info=True)
        return balances


def get_account_balance(account_name: str, asset: str, balances_map: dict | None = None) -> Decimal:
    logger.debug(f"Запрос баланса для {account_name} / {asset.upper()}")
    current_balances = balances_map
    if current_balances is None:
        logger.debug("balances_map не предоставлен, загружаем все балансы.")
        current_balances = get_all_balances()

    key = (account_name.strip().lower(), asset.strip().upper())
    balance_data = current_balances.get(key)

    if balance_data:
        return balance_data['balance']
    return Decimal('0')


def has_sufficient_balance(account_name: str, asset: str, required_amount: Decimal, balances_map: dict | None = None) -> bool:
    if not isinstance(required_amount, Decimal):
        try:
            required_amount = Decimal(str(required_amount))
        except InvalidOperation:
            logger.error(f"Некорректный required_amount '{required_amount}'.")
            return False

    current_balance = get_account_balance(
        account_name, asset, balances_map=balances_map)
    is_sufficient = current_balance >= required_amount

    log_level = logging.WARNING if not is_sufficient else logging.INFO
    logger.log(
        log_level, f"Проверка баланса: {account_name.lower()}/{asset.upper()}. Требуется: {required_amount}, Доступно: {current_balance}. {'НЕ ОК' if not is_sufficient else 'OK'}.")

    return is_sufficient


def find_position_by_symbol(symbol_to_find: str, exchange_name_to_find: str | None) -> tuple[int | None, dict | None]:
    open_positions = get_all_open_positions()
    for pos_data in open_positions:
        s_sym = str(pos_data.get('Symbol', '')).upper()
        s_ex = str(pos_data.get('Exchange', '')).strip()
        row_num = pos_data.get('row_number')
        if not s_sym or row_num is None:
            continue

        sym_match = s_sym == symbol_to_find.upper()
        ex_match = (s_ex.lower() == exchange_name_to_find.strip(
        ).lower()) if exchange_name_to_find else True

        if sym_match and ex_match:
            logger.info(
                f"Найдена позиция: {symbol_to_find} на '{exchange_name_to_find or s_ex}' в строке {row_num}.")
            return int(row_num), pos_data

    logger.info(
        f"Позиция для {symbol_to_find} на '{exchange_name_to_find or 'любой бирже'}' не найдена.")
    return None, None


def update_open_position_entry(row_number: int, new_net_amount: Decimal, new_avg_price: Decimal, exchange_name: str, symbol: str) -> bool:
    sheet_name = config.OPEN_POSITIONS_SHEET_NAME
    context_log = f"позиции {symbol} ({exchange_name}) строка {row_number}"
    try:
        sheet = get_open_positions_sheet()
        if not sheet:
            logger.error(
                f"Не удалось обновить {context_log} - лист не найден.")
            return False

        headers = get_headers(sheet_name)
        if not headers:
            logger.error(
                f"Не удалось обновить {context_log} - заголовки не найдены.")
            return False

        h_low = [h.lower().strip() for h in headers]
        net_a_key, avg_p_key = 'Net_Amount', 'Avg_Entry_Price'
        net_a_opts = OPEN_POSITIONS_FIELD_MAP.get(net_a_key, [net_a_key])
        avg_p_opts = OPEN_POSITIONS_FIELD_MAP.get(avg_p_key, [avg_p_key])
        net_a_idx = _find_col_index(
            h_low, [o.lower().strip() for o in net_a_opts])
        avg_p_idx = _find_col_index(
            h_low, [o.lower().strip() for o in avg_p_opts])

        if net_a_idx is None or avg_p_idx is None:
            logger.error(
                f"Колонки '{net_a_key}'/'{avg_p_key}' не найдены при обновлении {context_log}.")
            return False

        f_net_a, f_avg_p = format_value_for_sheet(
            new_net_amount), format_value_for_sheet(new_avg_price)
        payload = [{'range': gspread.utils.rowcol_to_a1(row_number, net_a_idx + 1), 'values': [[f_net_a]]}, {
            'range': gspread.utils.rowcol_to_a1(row_number, avg_p_idx + 1), 'values': [[f_avg_p]]},]

        sheet.batch_update(payload, value_input_option='USER_ENTERED')
        logger.info(f"Успешно обновлена {context_log}.")
        return True
    except gspread.exceptions.APIError as e_api:
        logger.error(
            f"Ошибка API при обновлении {context_log}. Ошибка: {e_api}", exc_info=True)
    except Exception as e:
        logger.error(
            f"Непредвиденная ошибка при обновлении {context_log}. Ошибка: {e}", exc_info=True)
    return False


def delete_row_from_sheet(sheet_name: str, row_number: int, context_for_log: str | None = None) -> bool:
    log_ctx_detail = context_for_log or f"строки {row_number} из '{sheet_name}'"
    try:
        sheet = get_sheet_by_name(sheet_name)
        if not sheet:
            logger.error(
                f"Не удалось удалить {log_ctx_detail} - лист не найден.")
            return False

        sheet.delete_rows(row_number)
        logger.info(f"Успешно удалена {log_ctx_detail}.")
        return True
    except gspread.exceptions.APIError as e_api:
        logger.error(
            f"Ошибка API при удалении {log_ctx_detail}. Ошибка: {e_api}", exc_info=True)
    except Exception as e:
        logger.error(
            f"Непредвиденная ошибка при удалении {log_ctx_detail}. Ошибка: {e}", exc_info=True)
    return False


def update_cell(sheet_name: str, cell_a1_notation: str, value) -> bool:
    context_log = f"ячейки {cell_a1_notation} в '{sheet_name}' значением '{value}'"
    try:
        sheet = get_sheet_by_name(sheet_name)
        if not sheet:
            logger.error(
                f"Не удалось обновить {context_log} - лист не найден.")
            return False

        val_write = format_value_for_sheet(value)
        sheet.update(cell_a1_notation, [[val_write]],
                     value_input_option='USER_ENTERED')
        logger.info(f"Успешно обновлена {context_log}.")
        return True
    except gspread.exceptions.APIError as e_api:
        logger.error(
            f"Ошибка API при обновлении {context_log}. Ошибка: {e_api}", exc_info=True)
    except Exception as e:
        logger.error(
            f"Непредвиденная ошибка при обновлении {context_log}. Ошибка: {e}", exc_info=True)
    return False


def read_cell_from_sheet(sheet_name: str, cell_a1_notation: str) -> str | None:
    context_log = f"ячейки {cell_a1_notation} из '{sheet_name}'"
    try:
        sheet = get_sheet_by_name(sheet_name)
        if not sheet:
            logger.error(
                f"Не удалось прочитать {context_log} - лист не найден.")
            return None

        cell_obj = sheet.acell(cell_a1_notation)
        if cell_obj is None or cell_obj.value is None:
            logger.info(f"Ячейка {cell_a1_notation} в '{sheet_name}' пуста.")
            return None

        cell_value_str = str(cell_obj.value).strip()
        converted_from_serial = _convert_excel_serial_date_to_string(
            cell_value_str)
        if converted_from_serial:
            return converted_from_serial

        if not (re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", cell_value_str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}", cell_value_str)) and cell_value_str:
            logger.debug(
                f"Значение из '{sheet_name}'/{cell_a1_notation} ('{cell_value_str}') не дата.")

        return cell_value_str
    except gspread.exceptions.APIError as e_api:
        logger.error(
            f"Ошибка API при чтении {context_log}. Ошибка: {e_api}", exc_info=True)
    except Exception as e:
        logger.error(
            f"Непредвиденная ошибка при чтении {context_log}. Ошибка: {e}", exc_info=True)
    return None


def batch_update_core_trades_fifo_fields(updates: list[dict]) -> bool:
    if not updates:
        return True
    sheet_name = config.CORE_TRADES_SHEET_NAME
    context_log = f"FIFO полей для {len(updates)} записей в '{sheet_name}'"
    try:
        sheet = get_core_trades_sheet()
        if not sheet:
            logger.error(
                f"Не удалось обновить {context_log} - лист не найден.")
            return False

        headers = get_headers(sheet_name)
        if not headers:
            logger.error(
                f"Не удалось обновить {context_log} - заголовки не найдены.")
            return False

        h_low = [h.lower().strip() for h in headers]
        cq_key, sp_key = 'Fifo_Consumed_Qty', 'Fifo_Sell_Processed'
        cq_idx = _find_col_index(
            h_low, [o.lower().strip() for o in CORE_TRADES_FIELD_MAP.get(cq_key, [cq_key])])
        sp_idx = _find_col_index(
            h_low, [o.lower().strip() for o in CORE_TRADES_FIELD_MAP.get(sp_key, [sp_key])])

        if cq_idx is None or sp_idx is None:
            logger.error(
                f"Колонки FIFO ('{cq_key}'/'{sp_key}') не найдены в {sheet_name}.")
            return False

        cq_col, sp_col = cq_idx + 1, sp_idx + 1
        batch_payload = []
        for item in updates:
            row_num = item.get('row_number')
            if not row_num:
                logger.warning(f"Пропуск FIFO: нет row_number в {item}")
                continue
            if cq_key in item and item.get(cq_key) is not None:
                batch_payload.append({'range': gspread.utils.rowcol_to_a1(
                    row_num, cq_col), 'values': [[format_value_for_sheet(item[cq_key])]]})
            if sp_key in item and item.get(sp_key) is not None:
                batch_payload.append({'range': gspread.utils.rowcol_to_a1(
                    row_num, sp_col), 'values': [[str(item[sp_key]).upper()]]})

        if not batch_payload:
            return True

        sheet.batch_update(batch_payload, value_input_option='USER_ENTERED')
        logger.info(
            f"Пакетно обновлены {context_log} ({len(batch_payload)} изменений).")
        return True
    except gspread.exceptions.APIError as e_api:
        logger.error(
            f"Ошибка API при обновлении {context_log}. Ошибка: {e_api}", exc_info=True)
    except Exception as e:
        logger.error(
            f"Непредвиденная ошибка при обновлении {context_log}. Ошибка: {e}", exc_info=True)
    return False


def batch_append_to_fifo_log(fifo_log_entries: list[list]) -> bool:
    if not fifo_log_entries:
        return True
    sheet_name = config.FIFO_LOG_SHEET_NAME
    context_log = f"{len(fifo_log_entries)} записей в '{sheet_name}'"
    try:
        sheet = get_fifo_log_sheet()
        if not sheet:
            logger.error(
                f"Не удалось пакетно добавить {context_log} - лист не найден.")
            return False

        processed_entries = [[format_value_for_sheet(
            item) for item in entry_row] for entry_row in fifo_log_entries]
        sheet.append_rows(processed_entries, value_input_option='USER_ENTERED')
        logger.info(f"Пакетно добавлено {context_log}.")
        return True
    except gspread.exceptions.APIError as e_api:
        logger.error(
            f"Ошибка API при добавлении {context_log}. Ошибка: {e_api}", exc_info=True)
    except Exception as e:
        logger.error(
            f"Непредвиденная ошибка при добавлении {context_log}. Ошибка: {e}", exc_info=True)
    return False


def _aggregate_balance_changes(updates_list: list[dict]) -> dict[tuple, Decimal]:
    """Агрегирует все изменения балансов из списка обновлений."""
    aggregated_changes: dict[tuple, Decimal] = {}
    for update in updates_list:
        key = (str(update['account']).strip().lower(),
               str(update['asset']).strip().upper())
        change_val = update.get('change', Decimal('0'))
        if not isinstance(change_val, Decimal):
            change_val = Decimal(str(change_val))
        aggregated_changes[key] = aggregated_changes.get(
            key, Decimal('0')) + change_val
    return aggregated_changes


def _prepare_balance_payloads(
    aggregated_changes: dict,
    balances_to_use: dict,
    headers: list,
    col_indices: dict
) -> tuple[list[dict], list[list[str]]]:
    """Готовит списки для пакетного обновления и добавления строк."""
    temp_new_balances: dict[tuple, Decimal] = {}
    for key, total_change in aggregated_changes.items():
        current_balance_data = balances_to_use.get(key)
        current_val = current_balance_data['balance'] if current_balance_data else Decimal(
            '0')
        temp_new_balances[key] = current_val + total_change

    batch_payload_update: list[dict] = []
    rows_to_append_data: list[list[str]] = []

    target_timezone_ss = timezone(timedelta(hours=config.TZ_OFFSET_HOURS))
    now_str = datetime.datetime.now(timezone.utc).astimezone(
        target_timezone_ss).strftime('%Y-%m-%d %H:%M:%S')
    zero_threshold = Decimal('1e-8')

    for key, new_balance_decimal in temp_new_balances.items():
        account_name, asset_name = key

        quantizer = Decimal(config.QTY_PRECISION_STR_LOGGING)
        if asset_name == getattr(config, 'BASE_CURRENCY', 'USD').upper():
            quantizer = Decimal(config.USD_PRECISION_STR_LOGGING)

        new_balance_quantized = new_balance_decimal.quantize(
            quantizer, rounding=ROUND_HALF_UP)
        is_new_balance_zero = abs(new_balance_quantized) < zero_threshold
        final_balance_val = Decimal(
            '0') if is_new_balance_zero else new_balance_quantized

        if key in balances_to_use:
            # Готовим обновление существующей строки
            row_num = balances_to_use[key]['row_num']
            batch_payload_update.extend([
                {'range': gspread.utils.rowcol_to_a1(row_num, col_indices['balance'] + 1), 'values': [
                    [format_value_for_sheet(final_balance_val)]]},
                {'range': gspread.utils.rowcol_to_a1(
                    row_num, col_indices['ts'] + 1), 'values': [[now_str]]}
            ])
        elif not is_new_balance_zero:
            # Готовим добавление новой строки
            new_row = [""] * len(headers)
            new_row[col_indices['account']] = account_name
            new_row[col_indices['asset']] = asset_name
            new_row[col_indices['balance']
                    ] = format_value_for_sheet(final_balance_val)
            new_row[col_indices['ts']] = now_str
            if 'entity_type' in col_indices and col_indices['entity_type'] is not None:
                entity_type_val = "EXCHANGE" if account_name in [e.lower() for e in getattr(
                    config, 'KNOWN_EXCHANGES', [])] else "WALLET" if account_name in [w.lower() for w in getattr(config, 'KNOWN_WALLETS', [])] else "INTERNAL_ACCOUNT"
                new_row[col_indices['entity_type']] = entity_type_val
            rows_to_append_data.append(new_row)

    return batch_payload_update, rows_to_append_data


def _cleanup_zero_balance_rows(sheet: gspread.Worksheet):
    """Находит и удаляет строки с нулевым балансом после всех обновлений."""
    logger.info("Проверка необходимости удаления нулевых строк...")
    try:
        zero_threshold = Decimal('1e-8')
        all_current_balances_after_update = get_all_balances()
        rows_to_delete = [data['row_num'] for data in all_current_balances_after_update.values(
        ) if abs(data['balance']) < zero_threshold]

        if rows_to_delete:
            rows_to_delete.sort(reverse=True)
            logger.info(
                f"Обнаружено {len(rows_to_delete)} строк с нулевым балансом для удаления.")
            for row_idx in rows_to_delete:
                sheet.delete_rows(row_idx)
                time.sleep(0.5)
                logger.info(
                    f"Удалена строка {row_idx} с нулевым балансом.")
        else:
            logger.info("Строки с нулевым балансом для удаления не найдены.")
    except (gspread.exceptions.APIError, gspread.exceptions.GSpreadException) as e:
        logger.error(
            f"Ошибка API/gspread при удалении строк с нулевым балансом: {e}", exc_info=True)
    except Exception as e:
        logger.error(
            f"Непредвиденная ошибка при удалении строк: {e}", exc_info=True)


def batch_update_balances(updates_list: list[dict], current_balances_map_arg: dict | None = None) -> bool:
    if not updates_list:
        return True

    sheet_name = config.ACCOUNT_BALANCES_SHEET_NAME
    context_log = f"балансов ({len(updates_list)} изменений) в '{sheet_name}'"

    try:
        sheet = get_account_balances_sheet()
        if not sheet:
            logger.error(
                f"Не удалось обновить {context_log} - лист не найден.")
            return False

        balances_to_use = current_balances_map_arg if current_balances_map_arg is not None else get_all_balances()
        if balances_to_use is None:
            logger.error(
                f"Не удалось получить текущие балансы для обновления {context_log}.")
            return False

        headers = get_headers(sheet_name)
        if not headers and not sheet.row_values(1):
            logger.warning(f"Лист {sheet_name} пуст. Создаем заголовки.")
            default_headers_list = [
                ACCOUNT_BALANCES_FIELD_MAP['Account_Name'][0],
                ACCOUNT_BALANCES_FIELD_MAP['Asset'][0],
                ACCOUNT_BALANCES_FIELD_MAP['Balance'][0],
                ACCOUNT_BALANCES_FIELD_MAP['Last_Updated_Timestamp'][0]
            ]
            sheet.update('A1', [default_headers_list],
                         value_input_option='USER_ENTERED')
            invalidate_header_cache(sheet_name)
            headers = get_headers(sheet_name)

        if not headers:
            logger.error(
                f"Не удалось получить или создать заголовки для {sheet_name}.")
            return False

        headers_lower = [h.lower().strip() for h in headers]
        col_indices = {
            'account': _find_col_index(headers_lower, [h.lower() for h in ACCOUNT_BALANCES_FIELD_MAP['Account_Name']]),
            'asset': _find_col_index(headers_lower, [h.lower() for h in ACCOUNT_BALANCES_FIELD_MAP['Asset']]),
            'balance': _find_col_index(headers_lower, [h.lower() for h in ACCOUNT_BALANCES_FIELD_MAP['Balance']]),
            'ts': _find_col_index(headers_lower, [h.lower() for h in ACCOUNT_BALANCES_FIELD_MAP['Last_Updated_Timestamp']]),
            'entity_type': _find_col_index(headers_lower, [h.lower() for h in ACCOUNT_BALANCES_FIELD_MAP['Entity_Type']])
        }
        if any(idx is None for idx in [col_indices['account'], col_indices['asset'], col_indices['balance'], col_indices['ts']]):
            logger.error(
                f"Ключевые колонки (Account, Asset, Balance, Timestamp) не найдены в {sheet_name}.")
            return False

        # --- ШАГ 1: Агрегация всех изменений ---
        aggregated_changes = _aggregate_balance_changes(updates_list)

        # --- ШАГ 2: Подготовка данных для Google ---
        update_payload, append_payload = _prepare_balance_payloads(
            aggregated_changes, balances_to_use, headers, col_indices)

        # --- ШАГ 3: Выполнение пакетных запросов ---
        api_calls_made = False
        if update_payload:
            logger.info(
                f"Обновление {len(update_payload)//2} существующих балансов...")
            sheet.batch_update(update_payload,
                               value_input_option='USER_ENTERED')
            api_calls_made = True
        if append_payload:
            logger.info(
                f"Добавление {len(append_payload)} новых балансов...")
            sheet.append_rows(append_payload,
                              value_input_option='USER_ENTERED')
            api_calls_made = True

        if api_calls_made:
            time.sleep(1)

        # --- ШАГ 4: Очистка строк с нулевым балансом ---
        _cleanup_zero_balance_rows(sheet)

        logger.info(f"Обновление {context_log} завершено.")
        return True

    except gspread.exceptions.APIError as api_e:
        logger.error(
            f"Ошибка API Google Sheets при обновлении {context_log}. Ошибка: {api_e}", exc_info=True)
    except Exception as e:
        logger.error(
            f"Непредвиденная критическая ошибка в batch_update_balances ({context_log}). Ошибка: {e}", exc_info=True)
    return False
