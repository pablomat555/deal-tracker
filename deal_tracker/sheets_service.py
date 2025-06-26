# deal_tracker/sheets_service.py
import gspread
import logging
import re
import streamlit as st
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import TypeVar, Type, Optional, List, Any, get_type_hints
from dateutil.parser import parse as parse_datetime
from oauth2client.service_account import ServiceAccountCredentials
import config
from models import AnalyticsData, PositionData, FifoLogData

logger = logging.getLogger(__name__)

T = TypeVar('T')
_gspread_client: Optional[gspread.Client] = None
_header_cache: dict[str, List[str]] = {}
FIELD_TO_SHEET_NAMES_MAP: dict[str, List[str]] = {
    'timestamp': ['Timestamp', 'Время', 'Дата'], 'symbol': ['Symbol', 'Тикер'], 'exchange': ['Exchange', 'Биржа'],
    'net_amount': ['Net_Amount', 'Кол-во'], 'avg_entry_price': ['Avg_Entry_Price', 'Средняя цена входа'],
    'buy_trade_id': ['Buy_Trade_ID', 'ID Покупки'], 'sell_trade_id': ['Sell_Trade_ID', 'ID Продажи'],
    'matched_qty': ['Matched_Qty', 'Сопоставленное Кол-во'], 'buy_price': ['Buy_Price', 'Цена Покупки'],
    'sell_price': ['Sell_Price', 'Цена Продажи'], 'fifo_pnl': ['Fifo_PNL', 'PNL FIFO'],
    'timestamp_closed': ['Timestamp_Closed', 'Время Закрытия'], 'date_generated': ['Date_Generated', 'Дата генерации'],
    'total_realized_pnl': ['Total_Realized_PNL', 'Реализованный PNL'], 'total_unrealized_pnl': ['Total_Unrealized_PNL', 'Нереализованный PNL'],
    'net_total_pnl': ['Net_Total_PNL', 'Чистый PNL'], 'total_trades_closed': ['Total_Trades_Closed', 'Закрыто сделок'],
    'win_rate_percent': ['Win_Rate_Percent', 'Винрейт, %'], 'profit_factor': ['Profit_Factor', 'Профит-фактор'],
    'net_invested_funds': ['Net_Invested_Funds', 'Чистые инвестиции'], 'total_equity': ['Total_Equity', 'Общий капитал']
}

def invalidate_cache():
    global _header_cache; _header_cache = {}; logger.info("[CACHE] Кэш заголовков очищен.")

def _get_client() -> gspread.Client:
    global _gspread_client
    if _gspread_client is None:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(config.GOOGLE_CREDS_JSON_PATH, scope)
        _gspread_client = gspread.authorize(creds)
    return _gspread_client

def _get_sheet_by_name(sheet_name: str) -> Optional[gspread.Worksheet]:
    try: return _get_client().open_by_key(config.SPREADSHEET_ID).worksheet(sheet_name)
    except Exception as e: logger.error(f"Ошибка доступа к листу '{sheet_name}': {e}"); return None

def _get_headers(sheet_name: str) -> List[str]:
    if sheet_name not in _header_cache:
        sheet = _get_sheet_by_name(sheet_name)
        if not sheet: return []
        _header_cache[sheet_name] = [str(h).strip() for h in sheet.row_values(1) if h]
    return _header_cache[sheet_name]

def _find_column_index(headers: list, field_key: str) -> int:
    headers_lower = [h.strip().lower() for h in headers]
    possible_names = FIELD_TO_SHEET_NAMES_MAP.get(field_key.lower(), [field_key])
    for name in possible_names:
        try: return headers_lower.index(name.lower())
        except ValueError: continue
    raise ValueError(f"Не найдена колонка для поля '{field_key}' в заголовках: {headers}")

def _parse_decimal(value: Any) -> Optional[Decimal]:
    if value is None or value == '': return None
    try: return Decimal(str(value).replace(' ', '').replace(',', '.'))
    except (InvalidOperation, TypeError): return None

def _build_model_from_row(row: List[str], headers: List[str], model_cls: Type[T], row_num: int) -> T:
    kwargs = {}
    for field_name, field_type in get_type_hints(model_cls).items():
        try:
            col_idx = _find_column_index(headers, field_name)
            raw_value = row[col_idx] if col_idx < len(row) else None
            is_optional = type(None) in getattr(field_type, '__args__', [])
            if raw_value is None or raw_value == '':
                if is_optional: kwargs[field_name] = None; continue
                else: raise ValueError("пустое значение для обязательного поля")
            origin_type = getattr(field_type, '__origin__', field_type)
            if origin_type is Decimal:
                parsed = _parse_decimal(raw_value)
                if parsed is None and not is_optional: raise ValueError("не удалось преобразовать в Decimal")
                kwargs[field_name] = parsed
            elif origin_type is datetime: kwargs[field_name] = parse_datetime(raw_value)
            elif origin_type is int: kwargs[field_name] = int(Decimal(raw_value))
            else: kwargs[field_name] = str(raw_value)
        except Exception as e: raise ValueError(f"Строка {row_num}, Поле '{field_name}', Значение '{raw_value}': {e}") from e
    return model_cls(**kwargs)

def get_all_records(sheet_name: str, model_cls: Type[T]) -> tuple[List[T], List[str]]:
    sheet = _get_sheet_by_name(sheet_name)
    if not sheet: return [], [f"Не удалось получить доступ к листу '{sheet_name}'"]
    records, errors = [], []
    try:
        headers = _get_headers(sheet_name)
        if not headers: return [], [f"Лист '{sheet_name}' не содержит заголовков."]
        for i, row_values in enumerate(sheet.get_all_values()[1:]):
            if not any(row_values): continue
            try:
                instance = _build_model_from_row(row_values, headers, model_cls, i + 2)
                if hasattr(instance, 'row_number'): instance.row_number = i + 2
                records.append(instance)
            except Exception as e: errors.append(f"Лист '{sheet_name}': {e}")
    except Exception as e: errors.append(f"Критическая ошибка при чтении листа '{sheet_name}': {e}")
    return records, errors

def get_all_open_positions() -> tuple[List[PositionData], List[str]]:
    return get_all_records(config.OPEN_POSITIONS_SHEET_NAME, PositionData)

def get_all_fifo_logs() -> tuple[List[FifoLogData], List[str]]:
    return get_all_records(config.FIFO_LOG_SHEET_NAME, FifoLogData)