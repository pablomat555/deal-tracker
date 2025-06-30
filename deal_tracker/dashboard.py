# deal_tracker/dashboard.py
import streamlit as st
import pandas as pd
import logging
import os
import sys
from decimal import Decimal
from typing import Any, List

# --- Настройка путей и импортов ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path: sys.path.insert(0, project_root)

from locales import t
import config
import dashboard_utils
from models import TradeData # Убедимся, что модель импортирована

# --- НАСТРОЙКА СТРАНИЦЫ И ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ---
st.set_page_config(layout="wide", page_title="Trading Dashboard")
logger = logging.getLogger(__name__)
CURRENCY_SYMBOLS = {'USD': '$', 'EUR': '€'}
display_currency = CURRENCY_SYMBOLS.get(config.BASE_CURRENCY, config.BASE_CURRENCY)

# --- УНИВЕРСАЛЬНЫЕ ФУНКЦИИ ---
def render_pnl_metric(label: str, value: Decimal):
    """Отображает метрику PNL в стилизованном блоке."""
    style = dashboard_utils.style_pnl_value(value)
    formatted_value = dashboard_utils.format_number(value, add_plus_sign=True, currency_symbol=display_currency)
    st.markdown(f"""<div style="padding: 5px; border: 1px solid #3a3a3a; border-radius: 8px; text-align: center; height: 100%;"><div style="font-size: 0.8em; color: #9ca3af;">{label}</div><div style="{style} font-size: 1.25em; font-weight: 600;">{formatted_value}</div></div>""", unsafe_allow_html=True)

def setup_filters(positions_df: pd.DataFrame, closed_trades_df: pd.DataFrame):
    """Настраивает и отображает фильтры в боковой панели."""
    with st.sidebar:
        lang_options=["ru", "en"]; current_lang=st.session_state.get("lang", "ru"); lang_index=lang_options.index(current_lang) if current_lang in lang_options else 0; lang=st.radio("🌐 Язык / Language", options=lang_options, index=lang_index, key='lang_radio'); st.session_state["lang"]=lang; st.divider(); st.header(t('filters_header'))
        all_exchanges=pd.concat([positions_df['exchange'], closed_trades_df['exchange']]).dropna().unique(); selected_exchanges=st.multiselect(t('filter_by_exchange'), sorted(list(all_exchanges)))
        all_symbols=pd.concat([positions_df['symbol'], closed_trades_df['symbol']]).dropna().unique(); selected_symbols=st.multiselect(t('filter_by_symbol'), sorted(list(all_symbols)))
        return selected_exchanges, selected_symbols

def display_capital_overview(latest_analytics: Any, total_equity: Decimal, unrealized_pnl: Decimal):
    """Отображает верхний блок с ключевыми метриками капитала."""
    if not latest_analytics: return
    realized_pnl = Decimal(latest_analytics.total_realized_pnl)
    net_pnl = realized_pnl + unrealized_pnl
    c1,c2,c3,c4,c5 = st.columns([2.5,2.5,2.5,2,2])
    with c1: st.metric(t('total_equity'), dashboard_utils.format_number(total_equity, currency_symbol=display_currency))
    with c2: st.metric(t('net_invested'), dashboard_utils.format_number(Decimal(latest_analytics.net_invested_funds), currency_symbol=display_currency))
    with c3: st.metric(t('total_pnl'), dashboard_utils.format_number(net_pnl, add_plus_sign=True, currency_symbol=display_currency))
    with c4: render_pnl_metric(t('realized_pnl'), realized_pnl)
    with c5: render_pnl_metric(t('unrealized_pnl'), unrealized_pnl)

def display_active_investments(df: pd.DataFrame, prices: dict, exchanges: list, symbols: list):
    """Отображает таблицу активных инвестиций с расчетами."""
    st.markdown("#### Активные инвестиции")
    if df.empty:
        st.info(t('no_open_positions'))
        return
    
    # Расчеты выполняются на копии DF, чтобы не изменять оригинал
    df_filtered = df.copy()
    if exchanges: df_filtered = df_filtered[df_filtered['exchange'].isin(exchanges)]
    if symbols: df_filtered = df_filtered[df_filtered['symbol'].isin(symbols)]
    
    if df_filtered.empty:
        st.info(t('no_open_positions_to_display'))
        return

    # Расчеты PNL и стоимости делаются здесь, на уже отфильтрованных данных
    df_filtered['current_value'] = df_filtered['net_amount'] * df_filtered['current_price']
    df_filtered['unrealized_pnl'] = (df_filtered['current_price'] - df_filtered['avg_entry_price']) * df_filtered['net_amount']
    
    total_val = df_filtered['current_value'].sum()
    df_filtered['share'] = (df_filtered['current_value'] / total_val * 100) if total_val > 0 else 0

    d = pd.DataFrame()
    d[t('col_symbol')]=df_filtered['symbol']
    d[t('col_exchange')]=df_filtered['exchange']
    d[t('col_qty')]=df_filtered['net_amount'].apply(lambda x:dashboard_utils.format_number(x,config.QTY_DISPLAY_PRECISION))
    d[t('col_avg_entry')]=df_filtered['avg_entry_price'].apply(lambda x:dashboard_utils.format_number(x,config.PRICE_DISPLAY_PRECISION))
    d[t('current_price')]=df_filtered['current_price'].apply(lambda x:dashboard_utils.format_number(x,config.PRICE_DISPLAY_PRECISION))
    d[t('col_value')]=df_filtered['current_value'].apply(lambda x:dashboard_utils.format_number(x,config.USD_DISPLAY_PRECISION,currency_symbol=display_currency))
    d[t('col_share_percent')]=df_filtered['share'].apply(lambda x:f"{dashboard_utils.format_number(x)}%")
    d[t('current_pnl')]=df_filtered['unrealized_pnl'].apply(lambda x:dashboard_utils.format_number(x,config.USD_DISPLAY_PRECISION,add_plus_sign=True))
    
    st.dataframe(d.style.applymap(dashboard_utils.style_pnl_value, subset=[t('current_pnl')]), hide_index=True, use_container_width=True)

def display_closed_trades(df: pd.DataFrame, core_trades: List[TradeData], exchanges: list, symbols: list):
    """Отображает таблицу закрытых сделок (FIFO)."""
    st.markdown(f"### {t('closed_trades_header')}")
    if df.empty:
        st.info(t('no_closed_deals_data'))
        return

    trade_id_to_exchange = {trade.trade_id: trade.exchange for trade in core_trades if trade.trade_id}
    df['exchange'] = df['sell_trade_id'].map(trade_id_to_exchange)
    
    df_filtered = df.copy()
    if exchanges: df_filtered = df_filtered[df_filtered['exchange'].isin(exchanges)]
    if symbols: df_filtered = df_filtered[df_filtered['symbol'].isin(symbols)]
    
    if df_filtered.empty:
        st.info(t('no_closed_deals_after_filter'))
        return
        
    df_filtered = df_filtered.sort_values(by='timestamp_closed', ascending=False)
    
    d = pd.DataFrame()
    d[t('col_symbol')]=df_filtered['symbol']
    d[t('col_exchange')]=df_filtered['exchange'].fillna('-')
    d[t('col_timestamp_closed')]=pd.to_datetime(df_filtered['timestamp_closed']).dt.strftime('%Y-%m-%d %H:%M')
    d[t('col_qty')]=df_filtered['matched_qty'].apply(lambda x:dashboard_utils.format_number(x,config.QTY_DISPLAY_PRECISION))
    d[t('col_buy_price')]=df_filtered['buy_price'].apply(lambda x:dashboard_utils.format_number(x,config.PRICE_DISPLAY_PRECISION))
    d[t('col_sell_price')]=df_filtered['sell_price'].apply(lambda x:dashboard_utils.format_number(x,config.PRICE_DISPLAY_PRECISION))
    d[t('col_pnl_fifo')]=df_filtered['fifo_pnl'].apply(lambda x:dashboard_utils.format_number(Decimal(x),config.USD_DISPLAY_PRECISION,add_plus_sign=True,currency_symbol=display_currency))
    
    st.dataframe(d.style.applymap(dashboard_utils.style_pnl_value, subset=[t('col_pnl_fifo')]), hide_index=True, use_container_width=True)

# --- ГЛАВНЫЙ КОД ---
all_data, all_errors = dashboard_utils.load_all_data_with_error_handling()
if all_errors:
    with st.expander("⚠️ Обнаружены ошибки при чтении данных из Google Sheets", expanded=True):
        for msg in all_errors: st.error(f"- {msg}")

# Извлекаем все необходимые данные
positions_data = all_data.get('open_positions', [])
closed_trades_data = all_data.get('fifo_logs', [])
analytics_data = all_data.get('analytics_history', [])
core_trades_data = all_data.get('core_trades', [])
account_balances = all_data.get('account_balances', [])

# Создаем базовые DataFrame
positions_df = pd.DataFrame([p.__dict__ for p in positions_data]) if positions_data else pd.DataFrame(columns=['symbol', 'exchange'])
closed_trades_df = pd.DataFrame([t.__dict__ for t in closed_trades_data]) if closed_trades_data else pd.DataFrame(columns=['symbol', 'exchange', 'sell_trade_id'])

# Настраиваем фильтры
selected_exchanges, selected_symbols = setup_filters(positions_df, closed_trades_df)

if st.sidebar.button(t('update_button')):
    st.cache_data.clear()
    dashboard_utils.invalidate_cache()
    st.rerun()

# --- [ИЗМЕНЕНО] РАСЧЕТ АКТУАЛЬНЫХ ДАННЫХ ---
current_prices = dashboard_utils.fetch_current_prices_for_all_exchanges(positions_data)

# Считаем живую стоимость крипто-позиций
total_crypto_value = Decimal('0')
total_unrealized_pnl = Decimal('0')
if not positions_df.empty:
    def get_price(row):
        return current_prices.get(str(row.get('exchange', '')).lower(), {}).get(row.get('symbol'), Decimal('0'))
    
    positions_df['current_price'] = positions_df.apply(get_price, axis=1)
    
    # Конвертируем колонки в Decimal для расчетов
    for col in ['net_amount', 'avg_entry_price', 'current_price']:
        positions_df[col] = positions_df[col].apply(Decimal)
    
    # Рассчитываем актуальные показатели для всего портфеля (до фильтрации)
    positions_df['current_value'] = positions_df['net_amount'] * positions_df['current_price']
    positions_df['unrealized_pnl'] = (positions_df['current_price'] - positions_df['avg_entry_price']) * positions_df['net_amount']
    
    total_crypto_value = positions_df['current_value'].sum()
    total_unrealized_pnl = positions_df['unrealized_pnl'].sum()

# Считаем общую стоимость стейблкоинов
total_stables_value = sum(Decimal(b.balance) for b in account_balances if b.asset in config.INVESTMENT_ASSETS and b.balance is not None)

# Считаем итоговый "живой" капитал
live_total_equity = total_crypto_value + total_stables_value

# --- Отображение ---
latest_analytics_obj = analytics_data[-1] if analytics_data else None
if latest_analytics_obj:
    # Передаем в обзор капитала "живые" данные
    display_capital_overview(latest_analytics_obj, live_total_equity, total_unrealized_pnl)
else:
    st.info(t('no_data_for_analytics'))

st.divider()
# Передаем DF с уже рассчитанными ценами и ценами в функцию отображения
display_active_investments(positions_df.copy(), current_prices, selected_exchanges, selected_symbols)
st.divider()
display_closed_trades(closed_trades_df.copy(), core_trades_data, selected_exchanges, selected_symbols)

