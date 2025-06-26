# deal_tracker/dashboard.py
import streamlit as st
import pandas as pd
import logging
import os
import sys
from decimal import Decimal
from typing import Any

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')); 
if project_root not in sys.path: sys.path.insert(0, project_root)

from locales import t
import config
import dashboard_utils

st.set_page_config(layout="wide", page_title="Trading Dashboard")
logger = logging.getLogger(__name__)
CURRENCY_SYMBOLS = {'USD': '$', 'EUR': '€'}
display_currency = CURRENCY_SYMBOLS.get(config.BASE_CURRENCY, config.BASE_CURRENCY)

def render_pnl_metric(label: str, value: Decimal):
    style = dashboard_utils.style_pnl_value(value)
    formatted_value = dashboard_utils.format_number(value, add_plus_sign=True, currency_symbol=display_currency)
    st.markdown(f"""<div style="padding: 5px; border: 1px solid #3a3a3a; border-radius: 8px; text-align: center; height: 100%;"><div style="font-size: 0.8em; color: #9ca3af;">{label}</div><div style="{style} font-size: 1.25em; font-weight: 600;">{formatted_value}</div></div>""", unsafe_allow_html=True)

def setup_filters(positions_df: pd.DataFrame, closed_trades_df: pd.DataFrame):
    with st.sidebar:
        lang_options=["ru", "en"]; current_lang=st.session_state.get("lang", "ru"); lang_index=lang_options.index(current_lang) if current_lang in lang_options else 0; lang=st.radio("🌐 Язык / Language", options=lang_options, index=lang_index, key='lang_radio'); st.session_state["lang"]=lang; st.divider(); st.header(t('filters_header'))
        all_exchanges=pd.concat([positions_df['exchange'], closed_trades_df['exchange']]).dropna().unique(); selected_exchanges=st.multiselect(t('filter_by_exchange'), sorted(list(all_exchanges)))
        all_symbols=pd.concat([positions_df['symbol'], closed_trades_df['symbol']]).dropna().unique(); selected_symbols=st.multiselect(t('filter_by_symbol'), sorted(list(all_symbols)))
        return selected_exchanges, selected_symbols

def display_capital_overview(latest_analytics: Any, unrealized_pnl: Decimal):
    if not latest_analytics: return
    realized_pnl, net_pnl = Decimal(latest_analytics.total_realized_pnl), Decimal(latest_analytics.total_realized_pnl) + unrealized_pnl
    c1,c2,c3,c4,c5 = st.columns([2.5,2.5,2.5,2,2])
    with c1: st.metric(t('total_equity'), dashboard_utils.format_number(Decimal(latest_analytics.total_equity), currency_symbol=display_currency))
    with c2: st.metric(t('net_invested'), dashboard_utils.format_number(Decimal(latest_analytics.net_invested_funds), currency_symbol=display_currency))
    with c3: st.metric(t('total_pnl'), dashboard_utils.format_number(net_pnl, add_plus_sign=True, currency_symbol=display_currency))
    with c4: render_pnl_metric(t('realized_pnl'), realized_pnl)
    with c5: render_pnl_metric(t('unrealized_pnl'), unrealized_pnl)

def display_active_investments(df: pd.DataFrame, prices: dict, exchanges: list, symbols: list) -> Decimal:
    if df.empty: return Decimal('0')
    if exchanges: df = df[df['exchange'].isin(exchanges)]
    if symbols: df = df[df['symbol'].isin(symbols)]
    if df.empty: st.info(t('no_open_positions_to_display')); return Decimal('0')
    def get_price(row): ex_id, sym = str(row.get('exchange','')).lower(), row.get('symbol'); return prices.get(ex_id, {}).get(sym, Decimal('0'))
    df['current_price'] = df.apply(get_price, axis=1)
    for col in ['net_amount', 'avg_entry_price', 'current_price']: df[col] = df[col].apply(Decimal)
    df['current_value'] = df['net_amount'] * df['current_price']; df['unrealized_pnl'] = (df['current_price'] - df['avg_entry_price']) * df['net_amount']; total_val = df['current_value'].sum(); df['share'] = (df['current_value'] / total_val * 100) if total_val > 0 else 0
    d = pd.DataFrame(); d[t('col_symbol')]=df['symbol']; d[t('col_exchange')]=df['exchange']; d[t('col_qty')]=df['net_amount'].apply(lambda x:dashboard_utils.format_number(x,config.QTY_DISPLAY_PRECISION)); d[t('col_avg_entry')]=df['avg_entry_price'].apply(lambda x:dashboard_utils.format_number(x,config.PRICE_DISPLAY_PRECISION)); d[t('current_price')]=df['current_price'].apply(lambda x:dashboard_utils.format_number(x,config.PRICE_DISPLAY_PRECISION)); d[t('col_value')]=df['current_value'].apply(lambda x:dashboard_utils.format_number(x,config.USD_DISPLAY_PRECISION,currency_symbol=display_currency)); d[t('col_share_percent')]=df['share'].apply(lambda x:f"{dashboard_utils.format_number(x)}%"); d[t('current_pnl')]=df['unrealized_pnl'].apply(lambda x:dashboard_utils.format_number(x,config.USD_DISPLAY_PRECISION,add_plus_sign=True))
    st.dataframe(d.style.applymap(dashboard_utils.style_pnl_value, subset=[t('current_pnl')]), hide_index=True, use_container_width=True)
    return df['unrealized_pnl'].sum()

def display_closed_trades(df: pd.DataFrame, exchanges: list, symbols: list):
    st.markdown(f"### {t('closed_trades_header')}")
    if df.empty: st.info(t('no_closed_deals_data')); return
    if exchanges: df = df[df['exchange'].isin(exchanges)]
    if symbols: df = df[df['symbol'].isin(symbols)]
    if df.empty: st.info(t('no_closed_deals_after_filter')); return
    df = df.sort_values(by='timestamp_closed', ascending=False)
    d = pd.DataFrame(); d[t('col_symbol')]=df['symbol']; d[t('col_exchange')]=df['exchange']; d[t('col_timestamp_closed')]=pd.to_datetime(df['timestamp_closed']).dt.strftime('%Y-%m-%d %H:%M'); d[t('col_qty')]=df['matched_qty'].apply(lambda x:dashboard_utils.format_number(x,config.QTY_DISPLAY_PRECISION)); d[t('col_buy_price')]=df['buy_price'].apply(lambda x:dashboard_utils.format_number(x,config.PRICE_DISPLAY_PRECISION)); d[t('col_sell_price')]=df['sell_price'].apply(lambda x:dashboard_utils.format_number(x,config.PRICE_DISPLAY_PRECISION)); d[t('col_pnl_fifo')]=df['fifo_pnl'].apply(lambda x:dashboard_utils.format_number(Decimal(x),config.USD_DISPLAY_PRECISION,add_plus_sign=True,currency_symbol=display_currency))
    st.dataframe(d.style.applymap(dashboard_utils.style_pnl_value, subset=[t('col_pnl_fifo')]), hide_index=True, use_container_width=True)

# --- ГЛАВНЫЙ КОД ---
all_data, all_errors = dashboard_utils.load_all_data_with_error_handling()
positions, closed_trades, analytics = all_data.get('open_positions',[]), all_data.get('fifo_logs',[]), all_data.get('analytics_history',[])
positions_df = pd.DataFrame([p.__dict__ for p in positions]) if positions else pd.DataFrame(columns=['symbol', 'exchange'])
closed_trades_df = pd.DataFrame([t.__dict__ for t in closed_trades]) if closed_trades else pd.DataFrame(columns=['symbol', 'exchange'])
selected_exchanges, selected_symbols = setup_filters(positions_df, closed_trades_df)

if all_errors:
    with st.expander("⚠️ Обнаружены ошибки при чтении данных из Google Sheets", expanded=True):
        for msg in all_errors: st.error(msg)
        st.warning("Дэшборд может отображать неполные данные. Исправьте ошибки в таблице и обновите страницу.")

if st.sidebar.button(t('update_button')):
    st.cache_data.clear(); dashboard_utils.invalidate_cache(); st.rerun()

current_prices = dashboard_utils.fetch_current_prices_for_all_exchanges(positions)
latest_analytics_obj = analytics[-1] if analytics else None
unrealized_pnl = display_active_investments(positions_df.copy(), current_prices, [], []) if not positions_df.empty else Decimal('0')

if latest_analytics_obj: display_capital_overview(latest_analytics_obj, unrealized_pnl)
else: st.info(t('no_data_for_analytics'))

st.divider()
st.markdown("#### Активные инвестиции")
display_active_investments(positions_df.copy(), current_prices, selected_exchanges, selected_symbols)
st.divider()
display_closed_trades(closed_trades_df.copy(), selected_exchanges, selected_symbols)