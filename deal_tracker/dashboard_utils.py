# deal_tracker/dashboard_utils.py
import logging
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List
from collections import defaultdict

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sheets_service
import config
import ccxt
# --- [ИСПРАВЛЕНО] Добавлены все используемые модели ---
from models import AnalyticsData, PositionData, TradeData, FifoLogData, MovementData, BalanceData

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
    """Вызывает одну пакетную функцию из sheets_service для получения всех данных для всех страниц."""
    logger.info("Загрузка всех данных для дэшборда через dashboard_utils (batch)...")
    
    # Словарь, определяющий какие листы и с какими моделями нужно загрузить
    sheets_to_fetch = {
        config.ANALYTICS_SHEET_NAME: AnalyticsData,
        config.OPEN_POSITIONS_SHEET_NAME: PositionData,
        config.FIFO_LOG_SHEET_NAME: FifoLogData,
        config.CORE_TRADES_SHEET_NAME: TradeData,
        config.FUND_MOVEMENTS_SHEET_NAME: MovementData,
        config.ACCOUNT_BALANCES_SHEET_NAME: BalanceData,
    }
    
    # Единый вызов для получения всех данных
    all_data_from_sheets, all_errors = sheets_service.batch_get_records(sheets_to_fetch)

    # Преобразуем данные в формат, ожидаемый дэшбордом
    all_data = {
        'analytics_history': all_data_from_sheets.get(config.ANALYTICS_SHEET_NAME, []),
        'open_positions': all_data_from_sheets.get(config.OPEN_POSITIONS_SHEET_NAME, []),
        'fifo_logs': all_data_from_sheets.get(config.FIFO_LOG_SHEET_NAME, []),
        'core_trades': all_data_from_sheets.get(config.CORE_TRADES_SHEET_NAME, []),
        'fund_movements': all_data_from_sheets.get(config.FUND_MOVEMENTS_SHEET_NAME, []),
        'account_balances': all_data_from_sheets.get(config.ACCOUNT_BALANCES_SHEET_NAME, []),
    }
    
    logger.info(f"Данные загружены. Обнаружено ошибок: {len(all_errors)}.")
    return all_data, all_errors


@st.cache_data(ttl=60)
def fetch_current_prices_for_all_exchanges(positions: List[PositionData]) -> Dict[str, Dict[str, Decimal]]:
    """
    [ИСПРАВЛЕНО] Загружает актуальные цены для всех позиций,
    используя нечувствительный к регистру подход.
    """
    if not positions:
        return {}

    symbols_by_exchange = defaultdict(list)
    for pos in positions:
        if pos.exchange and pos.symbol:
            # Приводим к нижнему регистру
            symbols_by_exchange[pos.exchange.lower()].append(pos.symbol)

    all_prices = defaultdict(dict)
    for exchange_id, symbols in symbols_by_exchange.items():
        try:
            exchange_class = getattr(ccxt, exchange_id)
            exchange = exchange_class()
            tickers = exchange.fetch_tickers(list(set(symbols)))
            for symbol, data in tickers.items():
                if data and 'last' in data:
                    # Ключ - exchange_id в нижнем регистре
                    all_prices[exchange_id][symbol] = Decimal(str(data['last']))
        except (ccxt.ExchangeNotFound, ccxt.NetworkError, ccxt.ExchangeError) as e:
            logger.warning(f"Не удалось получить цены для биржи {exchange_id}: {e}")
            continue
    return dict(all_prices)


def invalidate_cache():
    """Очищает кэш данных в sheets_service, чтобы принудительно перезапросить их из Google."""
    sheets_service.invalidate_cache()


def create_pie_chart(df: pd.DataFrame, title: str, names_col: str, values_col: str) -> go.Figure:
    """Создает и настраивает круговой график Plotly."""
    if df.empty or values_col not in df.columns or names_col not in df.columns:
        # Возвращаем пустую фигуру, если данных нет
        fig = go.Figure()
        fig.update_layout(title_text=title, annotations=[dict(text="Нет данных", showarrow=False)])
        return fig

    fig = px.pie(df, names=names_col, values=values_col, title=title, hole=0.4)
    fig.update_traces(
        textposition='inside',
        textinfo='percent+label',
        hovertemplate='<b>%{label}</b><br>Стоимость: %{value:,.2f}$<br>Доля: %{percent}<extra></extra>'
    )
    fig.update_layout(
        showlegend=False,
        margin=dict(l=10, r=10, t=50, b=10),
        title_x=0.5
    )
    return fig

def get_precision_for_asset(asset_symbol: str) -> str:
    """
    Возвращает строку точности для КОЛИЧЕСТВА в зависимости от типа актива.
    2 знака для стейблкоинов, больше - для остальных.
    """
    if asset_symbol.upper() in config.INVESTMENT_ASSETS:
        return '0.01' # 2 знака после запятой для стейблкоинов
    else:
        # Используем стандартную точность из конфига для остальных активов
        return config.QTY_DISPLAY_PRECISION

def get_price_precision(asset_symbol: str) -> str:
    """
    Возвращает строку точности для ЦЕНЫ в зависимости от типа актива.
    2 знака для стейблкоинов, больше - для остальных.
    """
    # Для самого стейблкоина цена всегда 1.00.
    if asset_symbol.upper() in config.INVESTMENT_ASSETS:
        return '0.00' 
    else:
        # Для всех криптовалютных пар используем точность из конфига
        return config.PRICE_DISPLAY_PRECISION