# deal_tracker/sheets_service.py
import gspread
import logging
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import TypeVar, Type, Optional, List, Dict, Any, get_type_hints

from dateutil.parser import parse as parse_datetime
from oauth2client.service_account import ServiceAccountCredentials

import config
from models import TradeData, MovementData, PositionData, BalanceData, FifoLogData, AnalyticsData

logger = logging.getLogger(__name__)

# --- Определение Generic Type для моделей ---
T = TypeVar('T')

# --- Словарь для сопоставления полей моделей с возможными названиями столбцов в Sheets ---
FIELD_TO_SHEET_NAMES_MAP: Dict[str, List[str]] = {
    # Общие поля
    'timestamp': ['Timestamp', 'Время', 'Дата', 'Время сделки', 'Время операции'],
    'symbol': ['Symbol', 'Тикер', 'Инструмент', 'Торговая Пара'],
    'exchange': ['Exchange', 'Биржа'],
    'type': ['Type', 'Тип', 'Тип сделки', 'Тип операции', 'Направление'],
    'amount': ['Amount', 'Количество', 'Объем', 'Сумма'],
    'price': ['Price', 'Цена'],
    'notes': ['Notes', 'Заметки', 'Примечание', 'Описание'],
    'commission': ['Commission', 'Комиссия'],
    'commission_asset': ['Commission_Asset', 'Валюта комиссии', 'Fee Asset'],
    # TradeData
    'trade_id': ['Trade_ID', 'ID Сделки'], 'order_id': ['Order_ID', 'ID ордера'],
    'total_quote_amount': ['Total_Quote_Amount', 'Объем в валюте котировки'], 'trade_pnl': ['Trade_PNL', 'PNL по сделке'],
    'fifo_consumed_qty': ['Fifo_Consumed_Qty', 'FIFO Потреблено'], 'fifo_sell_processed': ['Fifo_Sell_Processed', 'FIFO Продажа Обработана'],
    # MovementData
    'movement_id': ['Movement_ID', 'ID Движения'], 'asset': ['Asset', 'Актив', 'Валюта'],
    'source_name': ['Source_Name', 'Источник'], 'destination_name': ['Destination_Name', 'Назначение'],
    'fee_amount': ['Fee_Amount', 'Сумма комиссии'], 'fee_asset': ['Fee_Asset', 'Валюта комиссии'],
    'transaction_id_blockchain': ['Transaction_ID_Blockchain', 'TX ID'],
    # PositionData
    'net_amount': ['Net_Amount', 'Amount', 'Количество', 'Объем', 'Кол-во'],
    'avg_entry_price': ['Avg_Entry_Price', 'Avg Price', 'Средняя цена входа'],
    'current_price': ['Current_Price', 'Текущая цена'], 'unrealized_pnl': ['Unrealized_PNL', 'Unreal PNL', 'Нереализованный PNL'],
    'last_updated': ['Last_Updated', 'Последнее обновление'],
    # BalanceData
    'account_name': ['Account_Name', 'Счет'], 'balance': ['Balance', 'Баланс'], 'entity_type': ['Entity_Type', 'Тип счета'],
    # FifoLogData
    'buy_trade_id': ['Buy_Trade_ID', 'ID Покупки'], 'sell_trade_id': ['Sell_Trade_ID', 'ID Продажи'],
    'matched_qty': ['Matched_Qty', 'Сопоставленное Кол-во'], 'buy_price': ['Buy_Price', 'Цена Покупки'],
    'sell_price': ['Sell_Price', 'Цена Продажи'], 'fifo_pnl': ['Fifo_PNL', 'PNL FIFO'],
    'timestamp_closed': ['Timestamp_Closed', 'Время Закрытия'], 'buy_timestamp': ['Buy_Timestamp', 'Время Покупки'],
}

# --- Кэширование ---
_gspread_client: Optional[gspread.Client] = None
_header_cache: Dict[str, List[str]] = {}

# --- Вспомогательные функции ("Переводчики") ---


def _parse_decimal(value: Any) -> Optional[Decimal]:
    if value is None or value == '':
        return None
    try:
        return Decimal(str(value).replace(',', '.').strip())
    except (InvalidOperation, TypeError):
        logger.debug(f"Не удалось преобразовать '{value}' в Decimal.")
        return None


def _format_decimal(value: Optional[Decimal]) -> str:
    if value is None:
        return ""
    return str(value).replace('.', ',')


def _format_datetime(value: Optional[datetime]) -> str:
    if value is None:
        return ""
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _format_bool(value: Optional[bool]) -> str:
    if value is None:
        return ""
    return "TRUE" if value else "FALSE"

# --- Основная логика работы с Google Sheets ---


def _get_client() -> gspread.Client:
    global _gspread_client
    if _gspread_client is None:
        logger.info("Авторизация в Google API...")
        try:
            scope = ["https://spreadsheets.google.com/feeds",
                     "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                config.GOOGLE_CREDS_JSON_PATH, scope)
            _gspread_client = gspread.authorize(creds)
            logger.info("Авторизация в Google API успешна.")
        except Exception as e:
            logger.critical(
                f"Критическая ошибка авторизации Google: {e}", exc_info=True)
            raise
    return _gspread_client


def _get_sheet_by_name(sheet_name: str) -> Optional[gspread.Worksheet]:
    try:
        client = _get_client()
        spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
        return spreadsheet.worksheet(sheet_name)
    except (gspread.exceptions.SpreadsheetNotFound, gspread.exceptions.WorksheetNotFound) as e:
        logger.error(f"Лист '{sheet_name}' или таблица не найдены: {e}")
    except Exception as e:
        logger.error(
            f"Ошибка доступа к листу '{sheet_name}': {e}", exc_info=True)
    return None


def _get_headers(sheet_name: str) -> List[str]:
    if sheet_name not in _header_cache:
        sheet = _get_sheet_by_name(sheet_name)
        if not sheet:
            return []
        headers = sheet.row_values(1)
        _header_cache[sheet_name] = [str(h).strip() for h in headers if h]
    return _header_cache[sheet_name]


def _build_model_from_row(row: List[str], headers: List[str], model_cls: Type[T]) -> Optional[T]:
    model_fields = get_type_hints(model_cls)
    kwargs = {}
    headers_lower = [h.lower() for h in headers]

    for field_name, field_type in model_fields.items():
        possible_sheet_names = FIELD_TO_SHEET_NAMES_MAP.get(field_name, [
                                                            field_name])
        col_idx = -1
        for name in possible_sheet_names:
            try:
                col_idx = headers_lower.index(name.lower())
                break
            except ValueError:
                continue
        if col_idx == -1:
            continue
        raw_value = row[col_idx] if col_idx < len(row) else None
        try:
            origin_type = getattr(field_type, '__origin__', field_type)
            if origin_type is Decimal:
                kwargs[field_name] = _parse_decimal(raw_value)
            elif origin_type is datetime:
                kwargs[field_name] = parse_datetime(
                    raw_value) if raw_value else None
            elif origin_type is bool:
                kwargs[field_name] = str(raw_value).strip(
                ).upper() == 'TRUE' if raw_value else None
            elif origin_type is int:
                kwargs[field_name] = int(raw_value) if raw_value else None
            else:
                kwargs[field_name] = str(
                    raw_value) if raw_value is not None else None
        except (ValueError, TypeError) as e:
            logger.warning(
                f"Ошибка парсинга поля '{field_name}' со значением '{raw_value}': {e}")
            kwargs[field_name] = None
    if 'row_number' in model_fields:
        kwargs['row_number'] = -1
    try:
        return model_cls(**kwargs)
    except TypeError as e:
        logger.error(
            f"Ошибка создания модели {model_cls.__name__} с аргументами {kwargs}: {e}")
        return None


def get_all_records(sheet_name: str, model_cls: Type[T]) -> List[T]:
    sheet = _get_sheet_by_name(sheet_name)
    if not sheet:
        return []
    headers = _get_headers(sheet_name)
    if not headers:
        logger.warning(f"В листе '{sheet_name}' отсутствуют заголовки.")
        return []
    try:
        all_values = sheet.get_all_values()[1:]
        records = []
        for i, row_values in enumerate(all_values):
            if not any(row_values):
                continue
            model_instance = _build_model_from_row(
                row_values, headers, model_cls)
            if model_instance:
                if hasattr(model_instance, 'row_number'):
                    model_instance.row_number = i + 2
                records.append(model_instance)
        logger.info(
            f"Загружено {len(records)} записей из '{sheet_name}' как '{model_cls.__name__}'.")
        return records
    except Exception as e:
        logger.error(
            f"Ошибка при чтении данных с листа '{sheet_name}': {e}", exc_info=True)
        return []


def _model_to_row(record: Any, headers: List[str]) -> List[str]:
    """Преобразует объект модели в список строк для записи в таблицу."""
    row_to_append = []
    record_dict = record.__dict__
    for header in headers:
        formatted_value = ""
        field_name_found = None
        for f_name, possible_names in FIELD_TO_SHEET_NAMES_MAP.items():
            if header.lower() in [p.lower() for p in possible_names]:
                field_name_found = f_name
                break
        if field_name_found and field_name_found in record_dict:
            value = record_dict[field_name_found]
            if isinstance(value, Decimal):
                formatted_value = _format_decimal(value)
            elif isinstance(value, datetime):
                formatted_value = _format_datetime(value)
            elif isinstance(value, bool):
                formatted_value = _format_bool(value)
            elif value is not None:
                formatted_value = str(value)
        row_to_append.append(formatted_value)
    return row_to_append


def append_record(sheet_name: str, record: Any) -> bool:
    headers = _get_headers(sheet_name)
    if not headers:
        logger.error(
            f"Не удалось добавить запись в '{sheet_name}': нет заголовков.")
        return False
    row_to_append = _model_to_row(record, headers)
    try:
        sheet = _get_sheet_by_name(sheet_name)
        if not sheet:
            return False
        sheet.append_row(row_to_append, value_input_option='USER_ENTERED')
        logger.info(f"Запись успешно добавлена в '{sheet_name}'.")
        return True
    except Exception as e:
        logger.error(
            f"Ошибка добавления записи в '{sheet_name}': {e}", exc_info=True)
        return False

# --- Публичные функции для конкретных моделей (ЧТЕНИЕ И ДОБАВЛЕНИЕ) ---


def get_all_core_trades(
) -> List[TradeData]: return get_all_records(config.CORE_TRADES_SHEET_NAME, TradeData)


def get_all_fund_movements() -> List[MovementData]: return get_all_records(
    config.FUND_MOVEMENTS_SHEET_NAME, MovementData)


def get_all_open_positions() -> List[PositionData]: return get_all_records(
    config.OPEN_POSITIONS_SHEET_NAME, PositionData)
def get_all_balances() -> List[BalanceData]: return get_all_records(
    config.ACCOUNT_BALANCES_SHEET_NAME, BalanceData)


def get_all_fifo_logs(
) -> List[FifoLogData]: return get_all_records(config.FIFO_LOG_SHEET_NAME, FifoLogData)


def add_trade(trade_data: TradeData) -> bool: return append_record(
    config.CORE_TRADES_SHEET_NAME, trade_data)


def add_movement(movement_data: MovementData) -> bool: return append_record(
    config.FUND_MOVEMENTS_SHEET_NAME, movement_data)


def add_position(position_data: PositionData) -> bool: return append_record(
    config.OPEN_POSITIONS_SHEET_NAME, position_data)

# --- НОВЫЕ ФУНКЦИИ (ОБНОВЛЕНИЕ, УДАЛЕНИЕ, ПАКЕТНАЯ ОБРАБОТКА) ---


def delete_row(sheet_name: str, row_number: int) -> bool:
    """Удаляет строку по ее номеру."""
    try:
        sheet = _get_sheet_by_name(sheet_name)
        if not sheet:
            return False
        sheet.delete_rows(row_number)
        logger.info(f"Строка {row_number} успешно удалена из '{sheet_name}'.")
        return True
    except Exception as e:
        logger.error(
            f"Ошибка удаления строки {row_number} из '{sheet_name}': {e}", exc_info=True)
        return False


def update_position(position: PositionData) -> bool:
    """Обновляет существующую открытую позицию."""
    if position.row_number is None:
        logger.error(
            f"Невозможно обновить позицию {position.symbol}, т.к. отсутствует номер строки.")
        return False
    try:
        sheet = _get_sheet_by_name(config.OPEN_POSITIONS_SHEET_NAME)
        if not sheet:
            return False

        headers = _get_headers(config.OPEN_POSITIONS_SHEET_NAME)
        row_values = _model_to_row(position, headers)

        # Преобразуем в формат для batch_update (список словарей)
        update_payload = [{'range': f'A{position.row_number}:{chr(ord("A")+len(headers)-1)}{position.row_number}',
                           'values': [row_values]}]

        sheet.batch_update(update_payload, value_input_option='USER_ENTERED')
        logger.info(
            f"Позиция {position.symbol} (строка {position.row_number}) успешно обновлена.")
        return True
    except Exception as e:
        logger.error(
            f"Ошибка обновления позиции {position.symbol}: {e}", exc_info=True)
        return False


def batch_update_balances(changes: List[Dict[str, Any]]) -> bool:
    """Пакетно обновляет балансы на основе списка изменений."""
    sheet_name = config.ACCOUNT_BALANCES_SHEET_NAME
    sheet = _get_sheet_by_name(sheet_name)
    if not sheet:
        return False

    current_balances = get_all_balances()
    balances_map: Dict[tuple[str, str], BalanceData] = {
        (b.account_name.lower(), b.asset.upper()): b for b in current_balances
    }

    balances_to_update: List[BalanceData] = []
    balances_to_add: List[BalanceData] = []

    for change in changes:
        account = change['account'].lower()
        asset = change['asset'].upper()
        change_amount = change['change']
        key = (account, asset)

        if key in balances_map:
            # Обновляем существующий баланс
            balance_obj = balances_map[key]
            balance_obj.balance += change_amount
            balance_obj.last_updated = datetime.now()
            balances_to_update.append(balance_obj)
        else:
            # Создаем новый баланс
            new_balance = BalanceData(
                account_name=account, asset=asset, balance=change_amount, last_updated=datetime.now()
            )
            balances_to_add.append(new_balance)
            # Добавляем в карту, чтобы последующие изменения в этом же батче его видели
            balances_map[key] = new_balance

    try:
        # Пакетное обновление существующих
        if balances_to_update:
            payload = []
            headers = _get_headers(sheet_name)
            for b in balances_to_update:
                if b.row_number:
                    payload.append({
                        'range': f'A{b.row_number}:{chr(ord("A")+len(headers)-1)}{b.row_number}',
                        'values': [_model_to_row(b, headers)]
                    })
            if payload:
                sheet.batch_update(payload, value_input_option='USER_ENTERED')
                logger.info(f"Пакетно обновлено {len(payload)} балансов.")

        # Добавление новых
        if balances_to_add:
            headers = _get_headers(sheet_name)
            rows_to_append = [_model_to_row(b, headers)
                              for b in balances_to_add]
            sheet.append_rows(
                rows_to_append, value_input_option='USER_ENTERED')
            logger.info(f"Добавлено {len(rows_to_append)} новых балансов.")

        # TODO: Добавить логику удаления строк с нулевым балансом, если требуется.

        return True
    except Exception as e:
        logger.error(
            f"Ошибка при пакетном обновлении балансов: {e}", exc_info=True)
        return False


def batch_append_fifo_logs(fifo_logs: List[FifoLogData]) -> bool:
    """Пакетно добавляет записи в Fifo_Log."""
    if not fifo_logs:
        return True
    sheet_name = config.FIFO_LOG_SHEET_NAME
    sheet = _get_sheet_by_name(sheet_name)
    if not sheet:
        return False

    headers = _get_headers(sheet_name)
    if not headers:
        return False

    try:
        rows_to_append = [_model_to_row(log, headers) for log in fifo_logs]
        sheet.append_rows(rows_to_append, value_input_option='USER_ENTERED')
        logger.info(
            f"Пакетно добавлено {len(rows_to_append)} записей в '{sheet_name}'.")
        return True
    except Exception as e:
        logger.error(
            f"Ошибка пакетного добавления в '{sheet_name}': {e}", exc_info=True)
        return False


def batch_update_trades_fifo_fields(updates: List[Dict[str, Any]]) -> bool:
    """Пакетно обновляет поля FIFO_Consumed_Qty и Fifo_Sell_Processed в Core_Trades."""
    if not updates:
        return True
    sheet_name = config.CORE_TRADES_SHEET_NAME
    sheet = _get_sheet_by_name(sheet_name)
    if not sheet:
        return False

    # Определяем нужные колонки
    headers = _get_headers(sheet_name)
    headers_lower = [h.lower() for h in headers]
    try:
        consumed_qty_col = headers_lower.index('fifoconsumedqty') + 1
        processed_col = headers_lower.index('fifosellprocessed') + 1
    except ValueError:
        logger.error(f"Не найдены обязательные FIFO колонки в '{sheet_name}'.")
        return False

    payload = []
    for update in updates:
        row_num = update.get('row_number')
        if not row_num:
            continue

        if 'fifo_consumed_qty' in update:
            range_str = gspread.utils.rowcol_to_a1(row_num, consumed_qty_col)
            value = _format_decimal(update['fifo_consumed_qty'])
            payload.append({'range': range_str, 'values': [[value]]})

        if 'fifo_sell_processed' in update:
            range_str = gspread.utils.rowcol_to_a1(row_num, processed_col)
            value = _format_bool(update['fifo_sell_processed'])
            payload.append({'range': range_str, 'values': [[value]]})

    if not payload:
        return True

    try:
        sheet.batch_update(payload, value_input_option='USER_ENTERED')
        logger.info(
            f"Пакетно обновлены FIFO поля для {len(updates)} записей в '{sheet_name}'.")
        return True
    except Exception as e:
        logger.error(
            f"Ошибка пакетного обновления FIFO полей: {e}", exc_info=True)
        return False


def add_analytics_record(analytics_data: AnalyticsData) -> bool:
    return append_record(config.ANALYTICS_SHEET_NAME, analytics_data)


def batch_update_positions(positions: List[PositionData]) -> bool:
    """Пакетно обновляет данные по списку открытых позиций (цены и PnL)."""
    if not positions:
        return True
    sheet_name = config.OPEN_POSITIONS_SHEET_NAME
    sheet = _get_sheet_by_name(sheet_name)
    if not sheet:
        return False

    headers = _get_headers(sheet_name)
    if not headers:
        return False

    payload = []
    for pos in positions:
        if pos.row_number:
            row_values = _model_to_row(pos, headers)
            # Обновляем всю строку, чтобы сохранить консистентность
            range_str = f'A{pos.row_number}:{chr(ord("A")+len(headers)-1)}{pos.row_number}'
            payload.append({'range': range_str, 'values': [row_values]})

    if not payload:
        return True

    try:
        sheet.batch_update(payload, value_input_option='USER_ENTERED')
        logger.info(
            f"Пакетно обновлены данные для {len(payload)} открытых позиций.")
        return True
    except Exception as e:
        logger.error(
            f"Ошибка пакетного обновления открытых позиций: {e}", exc_info=True)
        return False


def update_system_status(status: str, timestamp: datetime) -> bool:
    """Обновляет ячейки статуса в листе System_Status."""
    sheet_name = config.SYSTEM_STATUS_SHEET_NAME
    sheet = _get_sheet_by_name(sheet_name)
    if not sheet:
        return False

    try:
        # Обновляем две ячейки за один вызов, если они рядом (для эффективности)
        # или по отдельности, если они в разных местах
        payload = [
            {'range': config.UPDATER_LAST_RUN_CELL,
                'values': [[_format_datetime(timestamp)]]},
            {'range': config.UPDATER_STATUS_CELL, 'values': [[status]]}
        ]
        sheet.batch_update(payload, value_input_option='USER_ENTERED')
        logger.info(
            f"Статус системы обновлен: {status} в {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        return True
    except Exception as e:
        logger.error(f"Ошибка обновления статуса системы: {e}", exc_info=True)
        return False
