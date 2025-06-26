# deal_tracker/sheets_service.py
import gspread
import logging
import time
import re
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import TypeVar, Type, Optional, List, Dict, Any, get_type_hints

from dateutil.parser import parse as parse_datetime
from oauth2client.service_account import ServiceAccountCredentials

import config
from models import TradeData, MovementData, PositionData, BalanceData, FifoLogData, AnalyticsData

logger = logging.getLogger(__name__)

# --- КЭШИРОВАНИЕ ---
T = TypeVar('T')
_gspread_client: Optional[gspread.Client] = None
_header_cache: Dict[str, List[str]] = {}
_data_cache: Dict[str, tuple[List[Any], float]] = {}
CACHE_DURATION_SECONDS = 5

# --- Карта сопоставления полей и названий столбцов ---
FIELD_TO_SHEET_NAMES_MAP: Dict[str, List[str]] = {
    # ... (содержимое этой карты остается без изменений, как в вашем файле)
    'timestamp': ['Timestamp', 'Время', 'Дата', 'Время сделки', 'Время операции'],
    'symbol': ['Symbol', 'Тикер', 'Инструмент', 'Торговая Пара'],
    'exchange': ['Exchange', 'Биржа'],
    'amount': ['Amount', 'Количество', 'Объем', 'Сумма'],
    'price': ['Price', 'Цена'],
    'notes': ['Notes', 'Заметки', 'Примечание', 'Описание'],
    'trade_type': ['Type', 'Тип', 'Тип сделки', 'Направление'],
    'movement_type': ['Type', 'Тип', 'Тип операции'],
    'trade_id': ['Trade_ID', 'ID Сделки'],
    'order_id': ['Order_ID', 'ID ордера'],
    'total_quote_amount': ['Total_Quote_Amount', 'Объем в валюте котировки'],
    'trade_pnl': ['Trade_PNL', 'PNL по сделке'],
    'commission': ['Commission', 'Комиссия'],
    'commission_asset': ['Commission_Asset', 'Валюта комиссии', 'Fee Asset'],
    'movement_id': ['Movement_ID', 'ID Движения'],
    'asset': ['Asset', 'Актив', 'Валюта'],
    'source_name': ['Source_Name', 'Источник'],
    'destination_name': ['Destination_Name', 'Назначение'],
    'fee_amount': ['Fee_Amount', 'Сумма комиссии'],
    'fee_asset': ['Fee_Asset', 'Валюта комиссии'],
    'transaction_id_blockchain': ['Transaction_ID_Blockchain', 'TX ID'],
    'net_amount': ['Net_Amount', 'Количество', 'Объем', 'Кол-во'],
    'avg_entry_price': ['Avg_Entry_Price', 'Avg Price', 'Средняя цена входа'],
    'current_price': ['Current_Price', 'Текущая цена'],
    'unrealized_pnl': ['Unrealized_PNL', 'Unreal PNL', 'Нереализованный PNL'],
    'last_updated': ['Last_Updated', 'Последнее обновление'],
    'account_name': ['Account_Name', 'Счет'],
    'balance': ['Balance', 'Баланс'],
    'entity_type': ['Entity_Type', 'Тип счета'],
    'fifo_consumed_qty': ['Fifo_Consumed_Qty', 'FIFO Потреблено', 'fifoconsumedqty'],
    'fifo_sell_processed': ['Fifo_Sell_Processed', 'FIFO Продажа Обработана', 'fifosellprocessed'],
    'buy_trade_id': ['Buy_Trade_ID', 'ID Покупки'],
    'sell_trade_id': ['Sell_Trade_ID', 'ID Продажи'],
    'matched_qty': ['Matched_Qty', 'Сопоставленное Кол-во'],
    'buy_price': ['Buy_Price', 'Цена Покупки'],
    'sell_price': ['Sell_Price', 'Цена Продажи'],
    'fifo_pnl': ['Fifo_PNL', 'PNL FIFO'],
    'timestamp_closed': ['Timestamp_Closed', 'Время Закрытия'],
    'buy_timestamp': ['Buy_Timestamp', 'Время Покупки'],
    'date_generated': ['Date_Generated', 'Дата генерации'],
    'total_realized_pnl': ['Total_Realized_PNL', 'Реализованный PNL'],
    'total_unrealized_pnl': ['Total_Unrealized_PNL', 'Нереализованный PNL'],
    'net_total_pnl': ['Net_Total_PNL', 'Чистый PNL'],
    'total_trades_closed': ['Total_Trades_Closed', 'Закрыто сделок'],
    'winning_trades_closed': ['Winning_Trades_Closed', 'Прибыльных сделок'],
    'losing_trades_closed': ['Losing_Trades_Closed', 'Убыточных сделок'],
    'win_rate_percent': ['Win_Rate_Percent', 'Винрейт, %'],
    'average_win_amount': ['Average_Win_Amount', 'Средняя прибыль'],
    'average_loss_amount': ['Average_Loss_Amount', 'Средний убыток'],
    'profit_factor': ['Profit_Factor', 'Профит-фактор'],
    'expectancy': ['Expectancy', 'Мат. ожидание'],
    'total_commissions_paid': ['Total_Commissions_Paid', 'Всего комиссий'],
    'net_invested_funds': ['Net_Invested_Funds', 'Чистые инвестиции'],
    'portfolio_current_value': ['Portfolio_Current_Value', 'Текущая стоимость портфеля'],
    'total_equity': ['Total_Equity', 'Общий капитал']
}


def invalidate_cache(sheet_name: Optional[str] = None):
    # ... (код этой функции остается без изменений)
    global _data_cache
    if sheet_name:
        if sheet_name in _data_cache:
            del _data_cache[sheet_name]
            logger.info(f"[CACHE] Кэш для листа '{sheet_name}' очищен.")
    else:
        _data_cache = {}
        logger.info("[CACHE] Весь кэш данных очищен.")


# --- Приватные вспомогательные функции (без изменений) ---
def _parse_decimal(value: Any) -> Optional[Decimal]:
    # ... (код этой функции остается без изменений)
    if value is None or value == '':
        return None
    try:
        clean_value = str(value)
        clean_value = re.sub(r'[^\d,.-]', '', clean_value)
        if ',' in clean_value and '.' in clean_value:
            clean_value = clean_value.replace('.', '').replace(',', '.')
        elif ',' in clean_value:
            clean_value = clean_value.replace(',', '.')
        return Decimal(clean_value.strip())
    except (InvalidOperation, TypeError):
        return None


def _format_decimal(value: Optional[Decimal]) -> str:
    # ... (код этой функции остается без изменений)
    if value is None:
        return ""
    return str(value).replace('.', ',')


def _format_datetime(value: Optional[datetime]) -> str:
    # ... (код этой функции остается без изменений)
    if value is None:
        return ""
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _format_bool(value: Optional[bool]) -> str:
    # ... (код этой функции остается без изменений)
    if value is None:
        return ""
    return "TRUE" if value else "FALSE"


def _get_client() -> gspread.Client:
    # ... (код этой функции остается без изменений)
    global _gspread_client
    if _gspread_client is None:
        try:
            scope = ["https://spreadsheets.google.com/feeds",
                     "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                config.GOOGLE_CREDS_JSON_PATH, scope)
            _gspread_client = gspread.authorize(creds)
        except Exception as e:
            logger.critical(
                f"Критическая ошибка авторизации Google: {e}", exc_info=True)
            raise
    return _gspread_client


def _get_sheet_by_name(sheet_name: str) -> Optional[gspread.Worksheet]:
    # ... (код этой функции остается без изменений)
    try:
        client = _get_client()
        spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
        return spreadsheet.worksheet(sheet_name)
    except Exception as e:
        logger.error(f"Ошибка доступа к листу '{sheet_name}': {e}")
        return None


def _get_headers(sheet_name: str) -> List[str]:
    # ... (код этой функции остается без изменений)
    if sheet_name not in _header_cache:
        sheet = _get_sheet_by_name(sheet_name)
        if not sheet:
            return []
        _header_cache[sheet_name] = [str(h).strip()
                                     for h in sheet.row_values(1) if h]
    return _header_cache[sheet_name]


def _find_column_index(headers: list, field_key: str) -> int:
    # ... (код этой функции остается без изменений)
    headers_lower = [h.strip().lower() for h in headers]
    possible_names = FIELD_TO_SHEET_NAMES_MAP.get(
        field_key.lower(), [field_key])
    for name in possible_names:
        try:
            return headers_lower.index(name.lower())
        except ValueError:
            continue
    raise ValueError(
        f"Не найдена колонка для поля '{field_key}' в заголовках: {headers}")


# --- [ОТЛАДОЧНАЯ ВЕРСИЯ] ---
# Заменена оригинальная функция _build_model_from_row
def _build_model_from_row(row: List[str], headers: List[str], model_cls: Type[T], row_num_for_logging: int) -> Optional[T]:
    model_fields = get_type_hints(model_cls)
    kwargs = {}
    for field_name, field_type in model_fields.items():
        try:
            col_idx = _find_column_index(headers, field_name)
            raw_value = row[col_idx] if col_idx < len(row) else None

            # Пропускаем пустые необязательные поля
            if raw_value is None or raw_value == '':
                # Проверяем, является ли поле Optional
                if type(None) in getattr(field_type, '__args__', []):
                    kwargs[field_name] = None
                    continue
                # Если поле обязательное, но пустое - логируем и пропускаем строку
                else:
                    logger.warning(
                        f"[ОТЛАДКА] Пропуск строки {row_num_for_logging} ({model_cls.__name__}): Пустое значение для обязательного поля '{field_name}'")
                    return None

            origin_type = getattr(field_type, '__origin__', field_type)

            if origin_type is Decimal:
                parsed_value = _parse_decimal(raw_value)
                if parsed_value is None:
                    logger.warning(
                        f"[ОТЛАДКА] Пропуск строки {row_num_for_logging} ({model_cls.__name__}): не удалось распарсить Decimal для поля '{field_name}' из значения '{raw_value}'")
                    return None
                kwargs[field_name] = parsed_value
            elif origin_type is datetime:
                kwargs[field_name] = parse_datetime(raw_value)
            elif origin_type is bool:
                kwargs[field_name] = str(raw_value).strip().upper() == 'TRUE'
            elif origin_type is int:
                # Парсим через Decimal для надежности, затем в int
                parsed_decimal = _parse_decimal(raw_value)
                kwargs[field_name] = int(
                    parsed_decimal) if parsed_decimal is not None else None
            else:
                kwargs[field_name] = str(raw_value)

        except Exception as e:
            logger.error(
                f"[ОТЛАДКА] КРИТИЧЕСКАЯ ОШИБКА в строке {row_num_for_logging} ({model_cls.__name__}) при обработке поля '{field_name}' со значением '{raw_value}': {e}")
            return None  # Пропускаем строку, если парсинг любого поля вызвал исключение

    try:
        return model_cls(**kwargs)
    except Exception as e:
        logger.error(
            f"Ошибка создания модели {model_cls.__name__} с аргументами {kwargs}: {e}")
        return None


def _model_to_row(record: Any, headers: List[str]) -> List[str]:
    # ... (код этой функции остается без изменений)
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


def get_all_records(sheet_name: str, model_cls: Type[T]) -> List[T]:
    now = time.time()
    if sheet_name in _data_cache and (now - _data_cache[sheet_name][1] < CACHE_DURATION_SECONDS):
        logger.info(f"[CACHE] Возврат кэшированных данных для '{sheet_name}'.")
        return _data_cache[sheet_name][0]

    logger.info(
        f"[API_CALL] Кэш для '{sheet_name}' пуст или устарел. Запрос к Google API...")
    sheet = _get_sheet_by_name(sheet_name)
    if not sheet:
        return []

    try:
        headers = _get_headers(sheet_name)
        if not headers:
            return []
        all_values = sheet.get_all_values()[1:]
        records = []
        for i, row_values in enumerate(all_values):
            if not any(row_values):
                continue

            # --- [ИЗМЕНЕНО] ---
            # Вызываем отладочную версию и передаем номер строки (i + 2, т.к. нумерация с 1 и есть заголовок)
            model_instance = _build_model_from_row(
                row_values, headers, model_cls, i + 2)

            if model_instance:
                if hasattr(model_instance, 'row_number'):
                    model_instance.row_number = i + 2
                records.append(model_instance)
        _data_cache[sheet_name] = (records, now)
        return records
    except Exception as e:
        logger.error(
            f"Ошибка при чтении данных с листа '{sheet_name}': {e}", exc_info=True)
        return []


# --- Остальные функции (append_record, delete_row, публичные функции и т.д.) остаются без изменений ---
def append_record(sheet_name: str, record: Any) -> bool:
    # ...
    try:
        sheet = _get_sheet_by_name(sheet_name)
        if not sheet:
            return False
        headers = _get_headers(sheet_name)
        row_to_append = _model_to_row(record, headers)
        sheet.append_row(row_to_append, value_input_option='USER_ENTERED')
        invalidate_cache(sheet_name)
        return True
    except Exception as e:
        logger.error(
            f"Ошибка добавления записи в '{sheet_name}': {e}", exc_info=True)
        return False


def delete_row(sheet_name: str, row_number: int) -> bool:
    # ...
    try:
        sheet = _get_sheet_by_name(sheet_name)
        if not sheet:
            return False
        sheet.delete_rows(row_number)
        invalidate_cache(sheet_name)
        return True
    except Exception as e:
        logger.error(
            f"Ошибка удаления строки {row_number} из '{sheet_name}': {e}", exc_info=True)
        return False

# --- ПУБЛИЧНЫЕ ФУНКЦИИ ---


def get_all_core_trades() -> List[TradeData]:
    return get_all_records(config.CORE_TRADES_SHEET_NAME, TradeData)


def get_all_fund_movements() -> List[MovementData]:
    return get_all_records(config.FUND_MOVEMENTS_SHEET_NAME, MovementData)


def get_all_open_positions() -> List[PositionData]:
    return get_all_records(config.OPEN_POSITIONS_SHEET_NAME, PositionData)


def get_all_balances() -> List[BalanceData]:
    return get_all_records(config.ACCOUNT_BALANCES_SHEET_NAME, BalanceData)


def get_all_fifo_logs() -> List[FifoLogData]:
    return get_all_records(config.FIFO_LOG_SHEET_NAME, FifoLogData)

# ... и так далее, все остальные функции без изменений
