# deal_tracker/dashboard_utils.py
import logging
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List
from collections import defaultdict

import streamlit as st
import sheets_service
import config
import ccxt
# Добавлены все используемые модели
from models import AnalyticsData, PositionData, FifoLogData, TradeData

logger = logging.getLogger(__name__)


def format_number(value: Any, precision_str: str = "0.01", add_plus_sign: bool = False, currency_symbol: str = "") -> str:
    """Форматирует число в строку с заданной точностью и валютным символом."""
    try:
        val = Decimal(str(value))
        decimals = abs(Decimal(precision_str).as_tuple().exponent)
        # Форматирование с пробелом в качестве разделителя тысяч
        formatted_str = f"{val:,.{decimals}f}".replace(',', ' ')
        if add_plus_sign and val > 0:
            formatted_str = f"+{formatted_str}"
        if currency_symbol:
            # Добавляем неразрывный пробел для лучшего отображения
            formatted_str = f"{formatted_str}\u00A0{currency_symbol}"
        return formatted_str
    except (InvalidOperation, TypeError, ValueError):
        return "-"


def style_pnl_value(val: Any) -> str:
    """Возвращает CSS стиль цвета в зависимости от знака числа."""
    try:
        # Более надежное извлечение числового значения
        s_val = str(val).replace('%', '').replace('\u00A0', '').replace(' ', '').replace('+', '').replace('$', '').replace('€', '')
        val_decimal = Decimal(s_val)
    except (InvalidOperation, TypeError, ValueError):
        return ''
    if val_decimal > 0:
        return 'color: #16A34A;' # Зеленый
    elif val_decimal < 0:
        return 'color: #DC2626;' # Красный
    return 'color: #6B7280;' # Серый


@st.cache_data(ttl=300)
def load_all_data_with_error_handling() -> tuple[Dict[str, List[Any]], List[str]]:
    """[ИЗМЕНЕНО] Вызывает одну пакетную функцию из sheets_service для получения всех данных."""
    logger.info("Загрузка всех данных для дэшборда через dashboard_utils (batch)...")
    
    # Словарь, определяющий какие листы и с какими моделями нужно загрузить
    sheets_to_fetch = {
        config.ANALYTICS_SHEET_NAME: AnalyticsData,
        config.OPEN_POSITIONS_SHEET_NAME: PositionData,
        config.FIFO_LOG_SHEET_NAME: FifoLogData,
        config.CORE_TRADES_SHEET_NAME: TradeData
    }
    
    # Единый вызов для получения всех данных
    all_data_from_sheets, all_errors = sheets_service.batch_get_records(sheets_to_fetch)

    # Преобразуем данные в формат, ожидаемый дэшбордом
    all_data = {
        'analytics_history': all_data_from_sheets.get(config.ANALYTICS_SHEET_NAME, []),
        'open_positions': all_data_from_sheets.get(config.OPEN_POSITIONS_SHEET_NAME, []),
        'fifo_logs': all_data_from_sheets.get(config.FIFO_LOG_SHEET_NAME, []),
        'core_trades': all_data_from_sheets.get(config.CORE_TRADES_SHEET_NAME, []),
    }
    
    logger.info(f"Данные загружены. Обнаружено ошибок: {len(all_errors)}.")
    return all_data, all_errors


@st.cache_data(ttl=60)
def fetch_current_prices_for_all_exchanges(positions: List[PositionData]) -> dict:
    """Получает актуальные цены для всех позиций с соответствующих бирж."""
    if not positions:
        return {}
        
    symbols_by_exchange = defaultdict(list)
    for pos in positions:
        exchange_id = (pos.exchange or '').lower()
        symbol = pos.symbol
        if exchange_id and symbol:
            symbols_by_exchange[exchange_id].append(symbol)
            
    all_prices = defaultdict(dict)
    for exchange_id, symbols in symbols_by_exchange.items():
        try:
            exchange_class = getattr(ccxt, exchange_id, None)
            if not exchange_class:
                logger.warning(f"Биржа {exchange_id} не найдена в CCXT.")
                continue
            
            exchange = exchange_class()
            tickers = exchange.fetch_tickers(list(set(symbols)))
            
            for symbol, ticker in tickers.items():
                if ticker and ticker.get('last') is not None:
                    all_prices[exchange_id][symbol] = Decimal(
                        str(ticker['last']))
        except Exception as e:
            logger.error(
                f"Не удалось получить цены с биржи {exchange_id}: {e}")
            continue
            
    return dict(all_prices)


def invalidate_cache():
    """Очищает кэш данных в sheets_service, чтобы принудительно перезапросить их из Google."""
    sheets_service.invalidate_cache()
    # Примечание: st.cache_data очищается отдельно в main-скрипте командой st.cache_data.clear()