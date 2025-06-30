# deal_tracker/sheets_service.py
import gspread
import logging
import re
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import TypeVar, Type, Optional, List, Any, Dict, get_type_hints
from dateutil.parser import parse as parse_datetime
from oauth2client.service_account import ServiceAccountCredentials
import config
from models import TradeData, MovementData, PositionData, BalanceData, FifoLogData, AnalyticsData

logger = logging.getLogger(__name__)

T = TypeVar('T')
_gspread_client: Optional[gspread.Client] = None
_header_cache: dict[str, List[str]] = {}

# Полная и актуальная карта сопоставления полей и названий столбцов
FIELD_TO_SHEET_NAMES_MAP: dict[str, List[str]] = {
    'timestamp': ['Timestamp', 'Время', 'Дата'], 'symbol': ['Symbol', 'Тикер'], 'exchange': ['Exchange', 'Биржа'], 'notes': ['Notes', 'Заметки'],
    'net_amount': ['Net_Amount', 'Кол-во', 'Количество'], 'avg_entry_price': ['Avg_Entry_Price', 'Средняя цена входа'], 'current_price': ['Current_Price', 'Текущая цена'], 'unrealized_pnl': ['Unrealized_PNL', 'Нереализованный PNL'],
    'buy_trade_id': ['Buy_Trade_ID', 'ID Покупки'], 'sell_trade_id': ['Sell_Trade_ID', 'ID Продажи'], 'matched_qty': ['Matched_Qty', 'Сопоставленное Кол-во'],
    'buy_price': ['Buy_Price', 'Цена Покупки'], 'sell_price': ['Sell_Price', 'Цена Продажи'], 'fifo_pnl': ['Fifo_PNL', 'PNL FIFO'],
    'timestamp_closed': ['Timestamp_Closed', 'Время Закрытия'], 'buy_timestamp': ['Buy_Timestamp', 'Время Покупки'],
    'date_generated': ['Date_Generated', 'Дата генерации'], 'total_realized_pnl': ['Total_Realized_PNL', 'Реализованный PNL'],
    'total_unrealized_pnl': ['Total_Unrealized_PNL', 'Нереализованный PNL'], 'net_total_pnl': ['Net_Total_PNL', 'Чистый PNL'],
    'total_trades_closed': ['Total_Trades_Closed', 'Закрыто сделок'], 'win_rate_percent': ['Win_Rate_Percent', 'Винрейт, %'],
    'profit_factor': ['Profit_Factor', 'Профит-фактор'], 'net_invested_funds': ['Net_Invested_Funds', 'Чистые инвестиции'],
    'total_equity': ['Total_Equity', 'Общий капитал'], 'trade_id': ['Trade_ID'], 'trade_type': ['Type', 'Тип', 'Тип сделки'],
    'movement_type': ['Type', 'Тип', 'Тип операции'], 'asset': ['Asset', 'Актив', 'Валюта'], 'amount': ['Amount', 'Количество', 'Сумма'],
    'source_name': ['Source_Name', 'Источник', 'Откуда'], 'destination_name': ['Destination_Name', 'Назначение', 'Куда'],
    'balance': ['Balance', 'Баланс'], 'account_name': ['Account_Name', 'Счет'], 'entity_type': ['Entity_Type', 'Тип счета'],
    'fifo_consumed_qty': ['Fifo_Consumed_Qty'], 'fifo_sell_processed': ['Fifo_Sell_Processed'], 'last_updated': ['Last_Updated', 'Последнее обновление'],
    'commission':['Commission', 'Комиссия'], 'commission_asset': ['Commission_Asset', 'Валюта комиссии'], 'order_id': ['Order_ID'], 'total_quote_amount': ['Total_Quote_Amount'],
    'trade_pnl': ['Trade_PNL', 'PNL по сделке'], 'movement_id': ['Movement_ID'], 'fee_amount': ['Fee_Amount', 'Сумма комиссии'], 'fee_asset': ['Fee_Asset', 'Валюта комиссии'],
    'transaction_id_blockchain': ['Transaction_ID_Blockchain', 'TX ID']
}

# --- Управление кэшем и клиентом ---
def invalidate_cache(sheet_name: Optional[str] = None):
    """Очищает кэш заголовков и, при необходимости, сбрасывает gspread клиент."""
    global _header_cache, _gspread_client
    if sheet_name:
        if sheet_name in _header_cache:
            del _header_cache[sheet_name]
            logger.info(f"[CACHE] Кэш заголовков для листа '{sheet_name}' очищен.")
    else:
        _header_cache = {}
        _gspread_client = None
        logger.info("[CACHE] Весь кэш gspread и заголовков очищен. Соединение будет переустановлено.")

def _get_client() -> gspread.Client:
    """Инициализирует и возвращает gspread клиент."""
    global _gspread_client
    if _gspread_client is None:
        try:
            _gspread_client = gspread.service_account(filename=config.GOOGLE_CREDS_JSON_PATH)
        except Exception as e:
            logger.critical(f"Критическая ошибка авторизации Google: {e}", exc_info=True)
            raise
    return _gspread_client

# --- Вспомогательные функции ---
def _get_sheet_by_name(sheet_name: str) -> Optional[gspread.Worksheet]:
    try:
        return _get_client().open_by_key(config.SPREADSHEET_ID).worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        logger.error(f"Лист '{sheet_name}' не найден.")
        return None
    except Exception as e:
        logger.error(f"Ошибка доступа к листу '{sheet_name}': {e}")
        return None

def _get_headers(sheet_name: str) -> List[str]:
    if sheet_name not in _header_cache:
        sheet = _get_sheet_by_name(sheet_name)
        if not sheet: return []
        _header_cache[sheet_name] = [str(h).strip() for h in sheet.row_values(1) if h]
    return _header_cache[sheet_name]

def _find_column_index(headers: List[str], field_key: str) -> int:
    headers_lower = [h.strip().lower() for h in headers]
    possible_names = FIELD_TO_SHEET_NAMES_MAP.get(field_key.lower(), []) + [field_key]
    for name in possible_names:
        try:
            return headers_lower.index(name.lower())
        except ValueError:
            continue
    raise ValueError(f"Колонка для поля '{field_key}' не найдена в заголовках: {headers}")

def _parse_decimal(value: Any) -> Optional[Decimal]:
    if value is None or str(value).strip() == '': return None
    try:
        clean_value = str(value).replace(' ', '').replace('\u00A0', '').replace(',', '.')
        return Decimal(clean_value)
    except (InvalidOperation, TypeError):
        return None

# --- Функции для форматирования и записи ---
def _format_decimal(value: Optional[Decimal]) -> str:
    return str(value).replace('.', ',') if value is not None else ""

def _format_datetime(value: Optional[datetime]) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S") if value is not None else ""

def _format_bool(value: Optional[bool]) -> str:
    return "TRUE" if value else "FALSE" if value is not None else ""

def _model_to_row(record: Any, headers: List[str]) -> List[str]:
    row = []
    record_dict = record.__dict__
    for header in headers:
        formatted_value = ""
        # Находим первое соответствующее имя поля, чтобы избежать неоднозначности
        field_name = next((f_name for f_name, names in FIELD_TO_SHEET_NAMES_MAP.items() if header.lower() in [n.lower() for n in names] and f_name in record_dict), header.lower())
        
        if field_name in record_dict:
            value = record_dict[field_name]
            if isinstance(value, Decimal): formatted_value = _format_decimal(value)
            elif isinstance(value, datetime): formatted_value = _format_datetime(value)
            elif isinstance(value, bool): formatted_value = _format_bool(value)
            elif value is not None: formatted_value = str(value)
        row.append(formatted_value)
    return row

# --- Функции чтения ---
def _build_model_from_row(row: List[str], headers: List[str], model_cls: Type[T], row_num: int) -> T:
    kwargs = {}
    model_fields = get_type_hints(model_cls)
    for field_name, field_type in model_fields.items():
        if field_name == 'row_number': continue
        is_optional = type(None) in getattr(field_type, '__args__', [])
        try:
            col_idx = _find_column_index(headers, field_name)
            raw_value = row[col_idx] if col_idx < len(row) else None
            
            if raw_value is None or str(raw_value).strip() == '':
                if is_optional:
                    kwargs[field_name] = None
                    continue
                else: raise ValueError("пустое значение для обязательного поля")
            
            origin_type = getattr(field_type, '__origin__', field_type)
            if origin_type is Decimal:
                parsed = _parse_decimal(raw_value)
                if parsed is None and not is_optional: raise ValueError(f"не удалось преобразовать '{raw_value}' в Decimal")
                kwargs[field_name] = parsed
            elif origin_type is datetime:
                kwargs[field_name] = parse_datetime(raw_value)
            elif origin_type is int:
                parsed_decimal = _parse_decimal(raw_value)
                if parsed_decimal is None: raise ValueError(f"не удалось преобразовать '{raw_value}' в Int")
                kwargs[field_name] = int(parsed_decimal)
            else:
                kwargs[field_name] = str(raw_value)
        except ValueError as e:
            if "Колонка для поля" in str(e) and is_optional:
                kwargs[field_name] = None
                continue
            raise ValueError(f"Строка {row_num}, Поле '{field_name}': {e}") from e
    return model_cls(**kwargs)

def batch_get_records(sheets_to_fetch: Dict[str, Type[T]]) -> tuple[Dict[str, List[Any]], List[str]]:
    sheet_names = list(sheets_to_fetch.keys())
    all_data, all_errors = {name: [] for name in sheet_names}, []
    try:
        spreadsheet = _get_client().open_by_key(config.SPREADSHEET_ID)
        batch_get_results = spreadsheet.values_batch_get(sheet_names)
        value_ranges = {item['range'].split('!')[0].strip("'"): item for item in batch_get_results.get('valueRanges', [])}
        for sheet_name in sheet_names:
            if sheet_name not in value_ranges:
                all_errors.append(f"Лист '{sheet_name}' не был найден в ответе API."); continue
            all_values = value_ranges[sheet_name].get('values', [])
            if not all_values or len(all_values) < 2:
                logger.warning(f"Лист '{sheet_name}' пуст или содержит только заголовки."); continue
            headers, data_rows, model_cls = all_values[0], all_values[1:], sheets_to_fetch[sheet_name]
            records = []
            for j, row_values in enumerate(data_rows):
                if not any(row_values): continue
                row_num = j + 2
                try:
                    instance = _build_model_from_row(row_values, headers, model_cls, row_num)
                    if hasattr(instance, 'row_number'): instance.row_number = row_num
                    records.append(instance)
                except Exception as e: all_errors.append(f"Лист '{sheet_name}', строка {row_num}: {e}")
            all_data[sheet_name] = records
    except Exception as e:
        error_msg = f"Критическая ошибка при пакетном чтении листов: {e}"; logger.error(error_msg, exc_info=True); all_errors.append(error_msg)
    return all_data, all_errors

def get_all_records(sheet_name: str, model_cls: Type[T]) -> tuple[List[T], List[str]]:
    """Прокси-функция для обратной совместимости. Использует batch_get_records."""
    data, errors = batch_get_records({sheet_name: model_cls})
    return data.get(sheet_name, []), errors

# --- Публичные функции для записи, обновления и удаления ---
def append_record(sheet_name: str, record: Any) -> bool:
    try:
        sheet = _get_sheet_by_name(sheet_name)
        if not sheet: return False
        headers = _get_headers(sheet_name)
        if not headers: logger.error(f"Не удалось добавить в '{sheet_name}': нет заголовков."); return False
        sheet.append_row(_model_to_row(record, headers), value_input_option='USER_ENTERED')
        invalidate_cache(sheet_name)
        return True
    except Exception as e:
        logger.error(f"Ошибка добавления в '{sheet_name}': {e}", exc_info=True)
        return False

def delete_row(sheet_name: str, row_number: int) -> bool:
    try:
        sheet = _get_sheet_by_name(sheet_name)
        if not sheet: return False
        sheet.delete_rows(row_number)
        invalidate_cache(sheet_name)
        return True
    except Exception as e:
        logger.error(f"Ошибка удаления строки {row_number} из '{sheet_name}': {e}", exc_info=True)
        return False

def update_position(position: PositionData) -> bool:
    if not position.row_number: return False
    sheet_name = config.OPEN_POSITIONS_SHEET_NAME
    try:
        sheet = _get_sheet_by_name(sheet_name)
        headers = _get_headers(sheet_name)
        if not sheet or not headers: return False
        row_values = _model_to_row(position, headers)
        range_str = f'A{position.row_number}:{chr(ord("A") + len(headers) - 1)}{position.row_number}'
        sheet.batch_update([{'range': range_str, 'values': [row_values]}], value_input_option='USER_ENTERED')
        invalidate_cache(sheet_name)
        return True
    except Exception as e:
        logger.error(f"Ошибка обновления позиции {position.symbol}: {e}", exc_info=True)
        return False

def batch_update_balances(changes: List[Dict[str, Any]]) -> bool:
    sheet_name = config.ACCOUNT_BALANCES_SHEET_NAME
    sheet = _get_sheet_by_name(sheet_name)
    if not sheet: return False
    
    all_balances, errors = get_all_records(sheet_name, BalanceData)
    if errors:
        logger.error(f"Не удалось прочитать балансы перед обновлением: {errors}")
        return False
        
    balances_map = {(b.account_name.lower(), b.asset.upper()): b for b in all_balances}
    to_update, to_add = [], []

    for change in changes:
        key = (change['account'].lower(), change['asset'].upper())
        amount = change['change']
        if key in balances_map:
            balance_obj = balances_map[key]
            balance_obj.balance = (balance_obj.balance or Decimal(0)) + amount
            balance_obj.last_updated = datetime.now()
            if balance_obj not in to_update: to_update.append(balance_obj)
        else:
            new_balance = BalanceData(account_name=key[0].capitalize(), asset=key[1], balance=amount, last_updated=datetime.now())
            to_add.append(new_balance)
            balances_map[key] = new_balance
    try:
        headers = _get_headers(sheet_name)
        if to_update:
            update_payload = [{'range': f'A{b.row_number}:{chr(ord("A")+len(headers)-1)}{b.row_number}', 'values': [_model_to_row(b, headers)]} for b in to_update if b.row_number]
            if update_payload: sheet.batch_update(update_payload, value_input_option='USER_ENTERED')
        if to_add:
            add_payload = [_model_to_row(b, headers) for b in to_add]
            if add_payload: sheet.append_rows(add_payload, value_input_option='USER_ENTERED')
        invalidate_cache(sheet_name)
        return True
    except Exception as e:
        logger.critical(f"Критическая ошибка записи балансов: {e}", exc_info=True)
        return False

def batch_update_trades_fifo_fields(updates: List[Dict[str, Any]]) -> bool:
    if not updates: return True
    sheet_name = config.CORE_TRADES_SHEET_NAME
    try:
        sheet = _get_sheet_by_name(sheet_name)
        if not sheet: return False
        headers = _get_headers(sheet_name)
        consumed_qty_col = _find_column_index(headers, 'fifo_consumed_qty') + 1
        processed_col = _find_column_index(headers, 'fifo_sell_processed') + 1
        payload = []
        for update in updates:
            row_num = update.get('row_number')
            if not row_num: continue
            if 'fifo_consumed_qty' in update:
                range_str = gspread.utils.rowcol_to_a1(row_num, consumed_qty_col)
                payload.append({'range': range_str, 'values': [[_format_decimal(update['fifo_consumed_qty'])]]})
            if 'fifo_sell_processed' in update:
                range_str = gspread.utils.rowcol_to_a1(row_num, processed_col)
                payload.append({'range': range_str, 'values': [[_format_bool(update['fifo_sell_processed'])]]})
        if not payload: return True
        sheet.batch_update(payload, value_input_option='USER_ENTERED')
        invalidate_cache(sheet_name)
        return True
    except Exception as e:
        logger.error(f"Ошибка пакетного обновления FIFO полей: {e}", exc_info=True)
        return False

def update_system_status(status: str, timestamp: datetime) -> bool:
    sheet_name = config.SYSTEM_STATUS_SHEET_NAME
    try:
        sheet = _get_sheet_by_name(sheet_name)
        if not sheet: return False
        payload = [
            {'range': config.UPDATER_LAST_RUN_CELL, 'values': [[_format_datetime(timestamp)]]},
            {'range': config.UPDATER_STATUS_CELL, 'values': [[status]]}
        ]
        sheet.batch_update(payload, value_input_option='USER_ENTERED')
        return True
    except Exception as e:
        logger.error(f"Ошибка обновления статуса системы: {e}", exc_info=True)
        return False

# --- Публичные "ярлыки" для бота ---
def add_trade(trade_data: TradeData) -> bool: return append_record(config.CORE_TRADES_SHEET_NAME, trade_data)
def add_movement(movement_data: MovementData) -> bool: return append_record(config.FUND_MOVEMENTS_SHEET_NAME, movement_data)
def add_position(position_data: PositionData) -> bool: return append_record(config.OPEN_POSITIONS_SHEET_NAME, position_data)
def add_analytics_record(analytics_data: AnalyticsData) -> bool: return append_record(config.ANALYTICS_SHEET_NAME, analytics_data)

# --- Публичные функции чтения для совместимости ---
def get_all_open_positions() -> List[PositionData]:
    records, errors = get_all_records(config.OPEN_POSITIONS_SHEET_NAME, PositionData)
    if errors: logger.error(f"Ошибки при чтении Open_Positions: {errors}")
    return records

def get_all_balances() -> List[BalanceData]:
    records, errors = get_all_records(config.ACCOUNT_BALANCES_SHEET_NAME, BalanceData)
    if errors: logger.error(f"Ошибки при чтении Account_Balances: {errors}")
    return records

def get_all_core_trades() -> List[TradeData]:
    records, errors = get_all_records(config.CORE_TRADES_SHEET_NAME, TradeData)
    if errors: logger.error(f"Ошибки при чтении Core_Trades: {errors}")
    return records
