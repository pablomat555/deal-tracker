# deal_tracker/dashboard.py
import dashboard_utils
import config
from locales import t
import streamlit as st
import pandas as pd
import logging
import os
import sys
from decimal import Decimal

# --- Настройка путей и импортов ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


# --- НАСТРОЙКА СТРАНИЦЫ И ЛОГГЕР ---
# Название для вкладки браузера
st.set_page_config(layout="wide", page_title="Trading Dashboard")
logger = logging.getLogger(__name__)


# --- Вспомогательная функция для стилизации PNL ---
def render_pnl_metric(label: str, value: Decimal):
    """Отображает PNL в красивом блоке с цветовой подсветкой."""
    style = dashboard_utils.style_pnl_value(value)
    formatted_value = dashboard_utils.format_number(
        value, add_plus_sign=True, currency_symbol=config.BASE_CURRENCY)

    html = f"""
    <div style="padding: 5px; border: 1px solid #3a3a3a; border-radius: 8px; text-align: center; height: 100%;">
        <div style="font-size: 0.8em; color: #9ca3af;">{label}</div>
        <div style="{style} font-size: 1.25em; font-weight: 600;">{formatted_value}</div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


# --- БОКОВАЯ ПАНЕЛЬ С ФИЛЬТРАМИ ---
def setup_filters(positions_df: pd.DataFrame, closed_trades_df: pd.DataFrame):
    """Настраивает и отображает виджеты фильтров в боковой панели."""
    with st.sidebar:
        lang_options = ["ru", "en"]
        current_lang = st.session_state.get("lang", "ru")
        lang_index = lang_options.index(
            current_lang) if current_lang in lang_options else 0
        lang = st.radio("🌐 Язык / Language", options=lang_options,
                        index=lang_index, key='lang_radio')
        st.session_state["lang"] = lang
        st.divider()
        st.header(t('filters_header'))

        # Собираем уникальные значения для фильтров со всех данных
        all_exchanges = pd.concat(
            [positions_df['exchange'], closed_trades_df['exchange']]).dropna().unique()
        selected_exchanges = st.multiselect(
            label=t('filter_by_exchange'), options=sorted(list(all_exchanges)), default=[])

        all_symbols = pd.concat(
            [positions_df['symbol'], closed_trades_df['symbol']]).dropna().unique()
        selected_symbols = st.multiselect(
            label=t('filter_by_symbol'), options=sorted(list(all_symbols)), default=[])

        return selected_exchanges, selected_symbols


# --- ФУНКЦИИ ОТОБРАЖЕНИЯ ---
def display_capital_overview(latest_analytics: dict, unrealized_pnl_from_positions: Decimal):
    """Отображает верхний, переработанный блок с ключевыми метриками капитала."""
    if not latest_analytics:
        return

    realized_pnl = Decimal(latest_analytics.total_realized_pnl)
    net_pnl = realized_pnl + unrealized_pnl_from_positions

    col1, col2, col3, col_real, col_unreal = st.columns([2.5, 2.5, 2.5, 2, 2])

    with col1:
        st.metric(t('total_equity'), dashboard_utils.format_number(
            Decimal(latest_analytics.total_equity), currency_symbol=config.BASE_CURRENCY))
    with col2:
        st.metric(t('net_invested'), dashboard_utils.format_number(Decimal(
            latest_analytics.net_invested_funds), currency_symbol=config.BASE_CURRENCY))
    with col3:
        st.metric(t('total_pnl'), dashboard_utils.format_number(
            net_pnl, add_plus_sign=True, currency_symbol=config.BASE_CURRENCY))
    with col_real:
        render_pnl_metric(t('realized_pnl'), realized_pnl)
    with col_unreal:
        render_pnl_metric(t('unrealized_pnl'), unrealized_pnl_from_positions)


def display_active_investments(positions_df: pd.DataFrame, current_prices: dict, selected_exchanges: list, selected_symbols: list) -> Decimal:
    """Отображает таблицу активных инвестиций с учетом фильтров."""
    if positions_df.empty:
        st.info(t('no_open_positions'))
        return Decimal('0')

    # Применение фильтров
    if selected_exchanges:
        positions_df = positions_df[positions_df['exchange'].isin(
            selected_exchanges)]
    if selected_symbols:
        positions_df = positions_df[positions_df['symbol'].isin(
            selected_symbols)]

    if positions_df.empty:
        st.info(t('no_open_positions_to_display'))
        return Decimal('0')

    # Расчеты и форматирование
    def get_price(row):
        exchange_id = str(row.get('exchange', '')).lower()
        symbol = row.get('symbol')
        return current_prices.get(exchange_id, {}).get(symbol, Decimal('0'))

    positions_df['current_price'] = positions_df.apply(get_price, axis=1)
    for col in ['net_amount', 'avg_entry_price', 'current_price']:
        positions_df[col] = positions_df[col].apply(Decimal)

    positions_df['current_value'] = positions_df['net_amount'] * \
        positions_df['current_price']
    positions_df['unrealized_pnl'] = (
        positions_df['current_price'] - positions_df['avg_entry_price']) * positions_df['net_amount']

    total_portfolio_value = positions_df['current_value'].sum()
    positions_df['share'] = (positions_df['current_value'] /
                             total_portfolio_value * 100) if total_portfolio_value > 0 else 0

    df_display = pd.DataFrame()
    df_display[t('col_symbol')] = positions_df['symbol']
    df_display[t('col_exchange')] = positions_df['exchange']
    df_display[t('col_qty')] = positions_df['net_amount'].apply(
        lambda x: dashboard_utils.format_number(x, precision_str=config.QTY_DISPLAY_PRECISION))
    df_display[t('col_avg_entry')] = positions_df['avg_entry_price'].apply(
        lambda x: dashboard_utils.format_number(x, precision_str=config.PRICE_DISPLAY_PRECISION))
    df_display[t('current_price')] = positions_df['current_price'].apply(
        lambda x: dashboard_utils.format_number(x, precision_str=config.PRICE_DISPLAY_PRECISION))
    df_display[t('col_value')] = positions_df['current_value'].apply(lambda x: dashboard_utils.format_number(
        x, currency_symbol=config.BASE_CURRENCY, precision_str=config.USD_DISPLAY_PRECISION))
    df_display[t('col_share_percent')] = positions_df['share'].apply(
        lambda x: f"{dashboard_utils.format_number(x)}%")
    df_display[t('current_pnl')] = positions_df['unrealized_pnl'].apply(
        lambda x: dashboard_utils.format_number(x, add_plus_sign=True, precision_str=config.USD_DISPLAY_PRECISION))

    st.dataframe(df_display.style.applymap(dashboard_utils.style_pnl_value, subset=[
                 t('current_pnl')]), hide_index=True, use_container_width=True)
    return positions_df['unrealized_pnl'].sum()


def display_closed_trades(closed_trades_df: pd.DataFrame, selected_exchanges: list, selected_symbols: list):
    """Отображает таблицу закрытых сделок (FIFO) с учетом фильтров."""
    st.markdown(f"### {t('closed_trades_header')}")
    if closed_trades_df.empty:
        st.info(t('no_closed_deals_data'))
        return

    # Применение фильтров
    if selected_exchanges:
        closed_trades_df = closed_trades_df[closed_trades_df['exchange'].isin(
            selected_exchanges)]
    if selected_symbols:
        closed_trades_df = closed_trades_df[closed_trades_df['symbol'].isin(
            selected_symbols)]

    if closed_trades_df.empty:
        st.info(t('no_closed_deals_after_filter'))
        return

    # Сортировка по дате закрытия
    closed_trades_df = closed_trades_df.sort_values(
        by='timestamp_closed', ascending=False)

    df_display = pd.DataFrame()
    df_display[t('col_symbol')] = closed_trades_df['symbol']
    df_display[t('col_exchange')] = closed_trades_df['exchange']
    df_display[t('col_timestamp_closed')] = pd.to_datetime(
        closed_trades_df['timestamp_closed']).dt.strftime('%Y-%m-%d %H:%M')
    df_display[t('col_qty')] = closed_trades_df['matched_qty'].apply(
        lambda x: dashboard_utils.format_number(x, precision_str=config.QTY_DISPLAY_PRECISION))
    df_display[t('col_buy_price')] = closed_trades_df['buy_price'].apply(
        lambda x: dashboard_utils.format_number(x, precision_str=config.PRICE_DISPLAY_PRECISION))
    df_display[t('col_sell_price')] = closed_trades_df['sell_price'].apply(
        lambda x: dashboard_utils.format_number(x, precision_str=config.PRICE_DISPLAY_PRECISION))
    df_display[t('col_pnl_fifo')] = closed_trades_df['fifo_pnl'].apply(lambda x: dashboard_utils.format_number(
        Decimal(x), add_plus_sign=True, currency_symbol=config.BASE_CURRENCY, precision_str=config.USD_DISPLAY_PRECISION))

    st.dataframe(df_display.style.applymap(dashboard_utils.style_pnl_value, subset=[
                 t('col_pnl_fifo')]), hide_index=True, use_container_width=True)


# --- ГЛАВНЫЙ КОД ---
# 1. Загружаем все данные один раз
all_data = dashboard_utils.load_all_dashboard_data()
positions_data = all_data.get('open_positions', [])
closed_trades_data = all_data.get('fifo_logs', [])

# 2. Создаем DataFrame для удобства фильтрации
positions_df = pd.DataFrame([p.__dict__ for p in positions_data]
                            ) if positions_data else pd.DataFrame(columns=['symbol', 'exchange'])
closed_trades_df = pd.DataFrame([t.__dict__ for t in closed_trades_data]
                                ) if closed_trades_data else pd.DataFrame(columns=['symbol', 'exchange'])

# 3. Настраиваем и получаем значения фильтров
selected_exchanges, selected_symbols = setup_filters(
    positions_df, closed_trades_df)

# 4. Кнопка "Обновить"
if st.button(t('update_button')):
    st.cache_data.clear()
    dashboard_utils.invalidate_cache()  # Очищаем и кэш gspread
    st.rerun()

# 5. Получаем актуальные рыночные цены
current_prices = dashboard_utils.fetch_current_prices_for_all_exchanges(
    positions_data)

# 6. Предварительный расчет Unrealized PNL для верхнего блока
total_unrealized_pnl = Decimal('0')
if not positions_df.empty:
    temp_df = positions_df.copy()

    def get_price(row):
        exchange_id = str(row.get('exchange', '')).lower()
        symbol = row.get('symbol')
        return current_prices.get(exchange_id, {}).get(symbol, Decimal('0'))
    temp_df['current_price'] = temp_df.apply(get_price, axis=1)
    for col in ['net_amount', 'avg_entry_price', 'current_price']:
        temp_df[col] = temp_df[col].apply(Decimal)
    temp_df['unrealized_pnl'] = (
        temp_df['current_price'] - temp_df['avg_entry_price']) * temp_df['net_amount']
    total_unrealized_pnl = temp_df['unrealized_pnl'].sum()

# 7. Отображаем все блоки
analytics_history = all_data.get('analytics_history', [])
latest_analytics_obj = analytics_history[-1] if analytics_history else None

if latest_analytics_obj:
    display_capital_overview(latest_analytics_obj, total_unrealized_pnl)
else:
    st.info(t('no_data_for_analytics'))

st.divider()
display_active_investments(
    positions_df.copy(), current_prices, selected_exchanges, selected_symbols)
st.divider()
display_closed_trades(closed_trades_df.copy(),
                      selected_exchanges, selected_symbols)
