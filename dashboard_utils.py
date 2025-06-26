# deal_tracker/dashboard_utils.py
"""
Вспомогательные утилиты, предназначенные ИСКЛЮЧИТЕЛЬНО
для использования в Streamlit-дэшборде.
"""
import logging
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List
from collections import defaultdict

import streamlit as st
import sheets_service
import config
import ccxt  # <--- ДОБАВЛЕН ИМПОРТ CCXT
from models import AnalyticsData

logger = logging.getLogger(__name__)


# --- ВАШИ СУЩЕСТВУЮЩИЕ ФУНКЦИИ (ОСТАЮТСЯ БЕЗ ИЗМЕНЕНИЙ) ---

def format_number(value: Any, precision_str: str = "0.01", add_plus_sign: bool = False, currency_symbol: str = "") -> str:
    """
    Форматирует число в красивую строку с пробелами как разделителем тысяч.
    """
    try:
        # Используем Decimal для точности
        val = Decimal(str(value))
        decimals = abs(Decimal(precision_str).as_tuple().exponent)
        # Форматирование с пробелами как разделителями
        formatted_str = f"{val:,.{decimals}f}".replace(',', ' ')
        if add_plus_sign and val > 0:
            formatted_str = f"+{formatted_str}"
        if currency_symbol:
            formatted_str = f"{formatted_str} {currency_symbol}"
        return formatted_str
    except (InvalidOperation, TypeError, ValueError):
        return "-"


def style_pnl_value(val: Any) -> str:
    """Возвращает CSS-стиль для ячеек PNL в зависимости от значения."""
    try:
        # Пытаемся обработать число напрямую
        val_decimal = Decimal(val)
    except (InvalidOperation, TypeError, ValueError):
        try:
            # Если не вышло, пытаемся обработать как строку (удаляя форматирование)
            s_val = str(val).replace(' ', '').replace(
                ',', '.').replace('%', '').replace('$', '')
            val_decimal = Decimal(s_val)
        except (InvalidOperation, TypeError, ValueError):
            return ''  # Возвращаем пустой стиль, если значение некорректно

    if val_decimal > 0:
        return 'color: #16A34A;'  # Зеленый
    elif val_decimal < 0:
        return 'color: #DC2626;'  # Красный
    return 'color: #6B7280;'  # Серый


@st.cache_data(ttl=300)
def load_all_dashboard_data() -> Dict[str, List[Any]]:
    """
    Централизованно загружает все данные для дэшборда из Google Sheets.
    """
    logger.info("Загрузка всех данных для дэшборда...")
    data = {
        'analytics_history': sheets_service.get_all_records(config.ANALYTICS_SHEET_NAME, AnalyticsData),
        'open_positions': sheets_service.get_all_open_positions(),
        'core_trades': sheets_service.get_all_core_trades(),
        'fifo_logs': sheets_service.get_all_fifo_logs(),
        'fund_movements': sheets_service.get_all_fund_movements(),
        'account_balances': sheets_service.get_all_balances(),
    }
    logger.info("Данные для дэшборда успешно загружены.")
    return data


# --- НОВАЯ ФУНКЦИЯ ДЛЯ ПОЛУЧЕНИЯ ЦЕН С БИРЖ (ИНТЕГРИРОВАНА СЮДА) ---

@st.cache_data(ttl=60)
def fetch_current_prices_for_all_exchanges(positions: list) -> dict:
    """
    Получает актуальные рыночные цены для позиций, группируя их по биржам.
    Возвращает вложенный словарь: {'binance': {'BTC/USDT': price}, 'bybit': {'ETH/USDT': price}}
    """
    if not positions:
        return {}

    symbols_by_exchange = defaultdict(list)
    for pos in positions:
        # Убедимся, что у позиции есть атрибуты 'Exchange' и 'Symbol'
        try:
            exchange_id = pos.Exchange.lower()
            symbol = pos.Symbol
            if exchange_id and symbol:
                symbols_by_exchange[exchange_id].append(symbol)
        except AttributeError:
            # Обработка для словарей, если get_all_open_positions возвращает их
            exchange_id = pos.get('Exchange', '').lower()
            symbol = pos.get('Symbol')
            if exchange_id and symbol:
                symbols_by_exchange[exchange_id].append(symbol)

    all_prices = defaultdict(dict)
    logger.info(f"Запрос цен с бирж: {list(symbols_by_exchange.keys())}")

    for exchange_id, symbols in symbols_by_exchange.items():
        try:
            exchange_class = getattr(ccxt, exchange_id)
            exchange = exchange_class()

            unique_symbols = list(set(symbols))
            tickers = exchange.fetch_tickers(unique_symbols)

            for symbol, ticker in tickers.items():
                if ticker and ticker.get('last') is not None:
                    all_prices[exchange_id][symbol] = Decimal(
                        str(ticker['last']))

            logger.info(
                f"Успешно получены цены для {len(tickers)} символов с биржи {exchange_id}.")

        except Exception as e:
            logger.error(
                f"Не удалось получить цены с биржи {exchange_id}: {e}")
            continue

    return dict(all_prices)
