# deal_tracker/sheets_service.py
import gspread
import logging
import re
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import TypeVar, Type, Optional, List, Any, Dict, get_type_hints

from dateutil.parser import parse as parse_datetime

import config
from models import AnalyticsData, PositionData, FifoLogData, TradeData

logger = logging.getLogger(__name__)

T = TypeVar('T')
_gspread_client: Optional[gspread.Client] = None

# --- [ИСПРАВЛЕНО] Карта сопоставления полей и названий столбцов ---
FIELD_TO_SHEET_NAMES_MAP: dict[str, List[str]] = {
    # Общие
    'symbol': ['Symbol', 'Тикер', 'Инструмент'],
    'exchange': ['Exchange', 'Биржа'],
    'notes': ['Notes', 'Заметки', 'Примечание'],
    'trade_id': ['Trade_ID', 'ID Сделки'],
    
    # --- [ВОЗВРАЩЕНА НЕДОСТАЮЩАЯ СТРОКА] ---
    'trade_type': ['Type', 'Тип', 'Тип сделки', 'Направление'],

    # Позиции (Open_Positions)
    'net_amount': ['Net_Amount', 'Кол-во', 'Количество'], 
    'avg_entry_price': ['Avg_Entry_Price', 'Средняя цена входа'],
    
    # FIFO Логи (Fifo_Log)
    'buy_trade_id': ['Buy_Trade_ID', 'ID Покупки'], 
    'sell_trade_id': ['Sell_Trade_ID', 'ID Продажи'],
    'matched_qty': ['Matched_Qty', 'Сопоставленное Кол-во'], 
    'buy_price': ['Buy_Price', 'Цена Покупки'],
    'sell_price': ['Sell_Price', 'Цена Продажи'], 
    'fifo_pnl': ['Fifo_PNL', 'PNL FIFO'],
    'timestamp_closed': ['Timestamp_Closed', 'Время Закрытия'], 
    
    # Аналитика (Analytics)
    'date_generated': ['Date_Generated', 'Дата генерации'],
    'total_realized_pnl': ['Total_Realized_PNL', 'Реализованный PNL'], 
    'total_unrealized_pnl': ['Total_Unrealized_PNL', 'Нереализованный PNL'],
    'net_total_pnl': ['Net_Total_PNL', 'Чистый PNL'], 
    'total_trades_closed': ['Total_Trades_Closed', 'Закрыто сделок'],
    'win_rate_percent': ['Win_Rate_Percent', 'Винрейт, %'], 
    'profit_factor': ['Profit_Factor', 'Профит-фактор'],
    'net_invested_funds': ['Net_Invested_Funds', 'Чистые инвестиции'], 
    'total_equity': ['Total_Equity', 'Общий капитал'],
}

def invalidate_cache():
    """Сбрасывает gspread клиент для пересоздания соединения при следующем вызове."""
    global _gspread_client
    _gspread_client = None
    logger.info("[CACHE] Клиент gspread сброшен. Соединение будет переустановлено при следующем запросе.")

def _get_client() -> gspread.Client:
    """Инициализирует и возвращает gspread клиент, используя современный метод авторизации."""
    global _gspread_client
    if _gspread_client is None:
        try:
            logger.info("Создание нового gspread клиента через service_account...")
            _gspread_client = gspread.service_account(filename=config.GOOGLE_CREDS_JSON_PATH)
            logger.info("Клиент gspread успешно создан.")
        except Exception as e:
            logger.critical(f"Критическая ошибка авторизации Google: {e}", exc_info=True)
            raise
    return _gspread_client

def _find_column_index(headers: list, field_key: str) -> int:
    """Находит индекс столбца по его возможному названию."""
    headers_lower = [h.strip().lower() for h in headers]
    possible_names = FIELD_TO_SHEET_NAMES_MAP.get(field_key, []) + [field_key]
    for name in possible_names:
        try:
            return headers_lower.index(name.lower())
        except ValueError:
            continue
    raise ValueError(f"Колонка для поля '{field_key}' не найдена. Ожидалось одно из: {possible_names}")

def _parse_decimal(value: Any) -> Optional[Decimal]:
    """Преобразует значение в Decimal, обрабатывая разные форматы."""
    if value is None or value == '': return None
    try:
        return Decimal(str(value).replace(' ', '').replace(',', '.'))
    except (InvalidOperation, TypeError):
        return None

def _build_model_from_row(row: List[str], headers: List[str], model_cls: Type[T], row_num: int) -> T:
    """Строит Pydantic-подобную модель из строки таблицы, обрабатывая типы данных."""
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
                else:
                    raise ValueError("пустое значение для обязательного поля")

            origin_type = getattr(field_type, '__origin__', field_type)
            
            if origin_type is Decimal:
                parsed = _parse_decimal(raw_value)
                if parsed is None and not is_optional:
                    raise ValueError(f"не удалось преобразовать '{raw_value}' в Decimal")
                kwargs[field_name] = parsed
            elif origin_type is datetime:
                kwargs[field_name] = parse_datetime(raw_value)
            elif origin_type is int:
                kwargs[field_name] = int(_parse_decimal(raw_value))
            else:
                kwargs[field_name] = str(raw_value)
                
        except ValueError as e:
            if "Колонка для поля" in str(e) and is_optional:
                kwargs[field_name] = None
                continue
            raise ValueError(f"Строка {row_num}, Поле '{field_name}': {e}") from e
            
    return model_cls(**kwargs)

def batch_get_records(sheets_to_fetch: Dict[str, Type[T]]) -> tuple[Dict[str, List[Any]], List[str]]:
    """Загружает данные с нескольких листов за один API вызов."""
    sheet_names = list(sheets_to_fetch.keys())
    logger.info(f"Пакетный запрос данных для листов: {sheet_names}")
    
    all_data = {name: [] for name in sheet_names}
    all_errors = []

    try:
        client = _get_client()
        spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
        
        batch_get_results = spreadsheet.values_batch_get(sheet_names)
        
        value_ranges = {item['range'].split('!')[0].strip("'"): item for item in batch_get_results.get('valueRanges', [])}

        for sheet_name in sheet_names:
            if sheet_name not in value_ranges:
                msg = f"Лист '{sheet_name}' не был найден в ответе API. Возможно, он пуст или имя указано неверно."
                logger.warning(msg)
                all_errors.append(msg)
                continue

            all_values = value_ranges[sheet_name].get('values', [])
            
            if not all_values or len(all_values) < 2:
                logger.warning(f"Лист '{sheet_name}' пуст или содержит только заголовки.")
                continue

            headers = all_values[0]
            data_rows = all_values[1:]
            model_cls = sheets_to_fetch[sheet_name]
            
            records = []
            for j, row_values in enumerate(data_rows):
                if not any(row_values) or len(row_values) == 0: continue
                row_num = j + 2
                try:
                    instance = _build_model_from_row(row_values, headers, model_cls, row_num)
                    if hasattr(instance, 'row_number'):
                        instance.row_number = row_num
                    records.append(instance)
                except Exception as e:
                    all_errors.append(f"Лист '{sheet_name}': {e}")
            
            all_data[sheet_name] = records

    except gspread.exceptions.SpreadsheetNotFound:
        error_msg = f"Критическая ошибка: Таблица с ID '{config.SPREADSHEET_ID}' не найдена."
        logger.critical(error_msg)
        all_errors.append(error_msg)
    except Exception as e:
        error_msg = f"Критическая ошибка при пакетном чтении листов: {e}"
        logger.error(error_msg, exc_info=True)
        all_errors.append(error_msg)
        
    return all_data, all_errors