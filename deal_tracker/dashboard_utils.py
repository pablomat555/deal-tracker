# deal_tracker/dashboard_utils.py
import logging
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List
from collections import defaultdict

import streamlit as st
import sheets_service
import config
import ccxt
from models import AnalyticsData, PositionData

logger = logging.getLogger(__name__)


def format_number(value: Any, precision_str: str = "0.01", add_plus_sign: bool = False, currency_symbol: str = "") -> str:
    try:
        val = Decimal(str(value))
        decimals = abs(Decimal(precision_str).as_tuple().exponent)
        formatted_str = f"{val:,.{decimals}f}".replace(',', ' ')
        if add_plus_sign and val > 0:
            formatted_str = f"+{formatted_str}"
        if currency_symbol:
            formatted_str = f"{formatted_str} {currency_symbol}"
        return formatted_str
    except (InvalidOperation, TypeError, ValueError):
        return "-"


def style_pnl_value(val: Any) -> str:
    try:
        s_val = str(val).replace('%', '').replace(
            ' ', '').replace('+', '').replace('$', '')
        val_decimal = Decimal(s_val)
    except (InvalidOperation, TypeError, ValueError):
        return ''
    if val_decimal > 0:
        return 'color: #16A34A;'
    elif val_decimal < 0:
        return 'color: #DC2626;'
    return 'color: #6B7280;'


@st.cache_data(ttl=300)
def load_all_data_with_error_handling() -> tuple[Dict[str, List[Any]], List[str]]:
    logger.info("Загрузка всех данных для дэшборда через dashboard_utils...")
    analytics_records, analytics_err = sheets_service.get_all_records(
        config.ANALYTICS_SHEET_NAME, AnalyticsData)
    positions_records, positions_err = sheets_service.get_all_open_positions()
    fifo_records, fifo_err = sheets_service.get_all_fifo_logs()
    all_data = {'analytics_history': analytics_records,
                'open_positions': positions_records, 'fifo_logs': fifo_records}
    all_errors = analytics_err + positions_err + fifo_err
    logger.info(f"Данные загружены. Обнаружено ошибок: {len(all_errors)}.")
    return all_data, all_errors


@st.cache_data(ttl=60)
def fetch_current_prices_for_all_exchanges(positions: List[PositionData]) -> dict:
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
            exchange = getattr(ccxt, exchange_id)()
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
    sheets_service.invalidate_cache()
