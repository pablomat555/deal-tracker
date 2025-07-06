import streamlit as st
import pandas as pd
import logging
import os
import sys
from decimal import Decimal, InvalidOperation
from typing import Any, List
from datetime import datetime, timedelta

# --- Настройка путей и импортов ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path: sys.path.insert(0, project_root)

from locales import t
import config
import dashboard_utils
from models import TradeData
from deal_tracker import utils

# --- НАСТРОЙКА СТРАНИЦЫ И ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ---
st.set_page_config(layout="wide", page_title="Trading Dashboard")
logger = logging.getLogger(__name__)
CURRENCY_SYMBOLS = {'USD': '$', 'EUR': '€'}
display_currency = CURRENCY_SYMBOLS.get(config.BASE_CURRENCY, config.BASE_CURRENCY)

# --- ФУНКЦИИ ДЛЯ ЦВЕТОВОЙ ПОДСВЕТКИ ---
def style_pnl(val: str) -> str:
    """🎨 Стилизация PNL: зеленый для +, красный для -."""
    if isinstance(val, str):
        if val.startswith('+'):
            return 'color: #28a745;'  # Зеленый
        elif val.startswith('-'):
            return 'color: #dc3545;'  # Красный
    return ''

def style_sl(val: str) -> str:
    """🔴 Стилизация SL: всегда красный."""
    return 'color: #dc3545;' if val and val != '-' else ''

def style_tp(val: str) -> str:
    """🟢 Стилизация TP: всегда зеленый."""
    return 'color: #28a745;' if val and val != '-' else ''

def style_risk(val: str) -> str:
    """🟡 Стилизация Риска: всегда желтый."""
    return 'color: #ffc107;' if val and val != '-' else ''

# --- УНИВЕРСАЛЬНЫЕ ФУНКЦИИ ---
def render_pnl_metric(label: str, value: Decimal):
    style = dashboard_utils.style_pnl_value(value)
    formatted_value = dashboard_utils.format_number(value, add_plus_sign=True, currency_symbol=display_currency)
    st.markdown(f"""<div style="padding: 5px; border: 1px solid #3a3a3a; border-radius: 8px; text-align: center; height: 100%;"><div style="font-size: 0.8em; color: #9ca3af;">{label}</div><div style="{style} font-size: 1.25em; font-weight: 600;">{formatted_value}</div></div>""", unsafe_allow_html=True)

def setup_filters(core_trades_df: pd.DataFrame, closed_trades_df: pd.DataFrame):
    with st.sidebar:
        lang_options=["ru", "en"]; current_lang=st.session_state.get("lang", "ru"); lang_index=lang_options.index(current_lang) if current_lang in lang_options else 0; lang=st.radio("🌐 Язык / Language", options=lang_options, index=lang_index, key='lang_radio'); st.session_state["lang"]=lang
        st.divider()
        st.number_input(
            label=t('timezone_setting_label'), min_value=-12, max_value=14,
            value=st.session_state.get('tz_offset', config.TZ_OFFSET_HOURS),
            key='tz_offset', help=t('timezone_setting_help')
        )
        st.divider()
        st.header(t('filters_header'))
        all_exchanges=pd.concat([core_trades_df['exchange'], closed_trades_df['exchange']]).dropna().unique(); selected_exchanges=st.multiselect(t('filter_by_exchange'), sorted(list(all_exchanges)))
        all_symbols=pd.concat([core_trades_df['symbol'], closed_trades_df['symbol']]).dropna().unique(); selected_symbols=st.multiselect(t('filter_by_symbol'), sorted(list(all_symbols)))
        if st.button(t('update_button')): st.cache_data.clear(); dashboard_utils.invalidate_cache(); st.rerun()
        return selected_exchanges, selected_symbols

def display_capital_overview(latest_analytics: Any, total_equity: Decimal, unrealized_pnl: Decimal, equity_delta_str: str = None):
    if not latest_analytics: return
    realized_pnl = Decimal(latest_analytics.total_realized_pnl)
    net_pnl = realized_pnl + unrealized_pnl
    
    c1,c2,c3,c4 = st.columns(4)
    
    with c1: 
        st.metric(
            label=t('total_equity'), 
            value=dashboard_utils.format_number(total_equity, currency_symbol=display_currency),
            delta=equity_delta_str
        )
    with c2: 
        st.metric(
            t('net_invested'), 
            dashboard_utils.format_number(Decimal(latest_analytics.net_invested_funds), currency_symbol=display_currency)
        )
    with c3:
        render_pnl_metric(t('total_pnl'), net_pnl)
    with c4:
        render_pnl_metric(t('realized_pnl'), realized_pnl)

def display_open_lots_table(core_trades: List[TradeData], current_prices: dict, selected_exchanges: list, selected_symbols: list):
    st.markdown(f"### {t('open_trades_header')}")

    open_lots_data = []
    for trade in core_trades:
        if trade.trade_type == 'BUY':
            trade_amount = utils.parse_decimal(trade.amount) or Decimal('0')
            consumed_qty = utils.parse_decimal(trade.fifo_consumed_qty) or Decimal('0')
            
            if trade_amount > consumed_qty:
                open_lots_data.append(trade)

    if not open_lots_data:
        st.info(t('no_open_positions')); return

    df_lots = pd.DataFrame([lot.__dict__ for lot in open_lots_data])
    
    # Фильтрация
    if selected_exchanges: df_lots = df_lots[df_lots['exchange'].isin(selected_exchanges)]
    if selected_symbols: df_lots = df_lots[df_lots['symbol'].isin(selected_symbols)]
    if df_lots.empty: st.info(t('no_open_positions_to_display')); return
    
    # --- "САНИТАРНАЯ ОБРАБОТКА" ДАННЫХ ПЕРЕД РАСЧЕТАМИ ---
    for col in ['amount', 'fifo_consumed_qty', 'price', 'sl', 'tp1', 'tp2', 'tp3']:
        if col in df_lots.columns:
            df_lots[col] = df_lots[col].apply(lambda x: utils.parse_decimal(x))
        else:
             df_lots[col] = None
    
    df_lots['amount'] = df_lots['amount'].fillna(Decimal('0'))
    df_lots['fifo_consumed_qty'] = df_lots['fifo_consumed_qty'].fillna(Decimal('0'))
    df_lots['price'] = df_lots['price'].fillna(Decimal('0'))

    # Расчеты
    df_lots['unsold_qty'] = df_lots['amount'] - df_lots['fifo_consumed_qty']
    def get_price(row): return current_prices.get(row['exchange'].lower(), {}).get(row['symbol'], Decimal('0'))
    df_lots['current_price'] = df_lots.apply(get_price, axis=1)
    df_lots['pnl_usd'] = (df_lots['current_price'] - df_lots['price']) * df_lots['unsold_qty']
    df_lots['risk_usd'] = df_lots.apply(lambda row: (row['price'] - row['sl']) * row['unsold_qty'] if pd.notna(row['sl']) and row['sl'] > 0 else None, axis=1)
    
    df_lots = df_lots.sort_values(by='timestamp', ascending=False)
    
    # Форматирование для вывода
    df_display = pd.DataFrame()
    df_display[t('col_date_buy')] = pd.to_datetime(df_lots['timestamp']).dt.strftime('%Y-%m-%d %H:%M')
    df_display[t('col_asset')] = df_lots['symbol']
    df_display[t('col_exchange')] = df_lots['exchange']
    df_display[t('col_qty_deal')] = df_lots['amount'].apply(lambda x: dashboard_utils.format_number(x, config.QTY_DISPLAY_PRECISION))
    df_display[t('col_qty_left')] = df_lots['unsold_qty'].apply(lambda x: dashboard_utils.format_number(x, config.QTY_DISPLAY_PRECISION))
    df_display[t('col_buy_price')] = df_lots.apply(lambda r: dashboard_utils.format_number(r['price'], dashboard_utils.get_price_precision(r['symbol'])), axis=1)
    df_display[t('col_current_price')] = df_lots.apply(lambda r: dashboard_utils.format_number(r['current_price'], dashboard_utils.get_price_precision(r['symbol'])), axis=1)
    df_display[t('col_pnl_sum')] = df_lots['pnl_usd'].apply(lambda x: dashboard_utils.format_number(x, add_plus_sign=True, currency_symbol=display_currency))
    
    final_cols = list(df_display.columns)
    for key in ['sl', 'tp1', 'tp2', 'tp3']:
        col_name = t(f'col_{key}')
        if key in df_lots.columns and df_lots[key].notna().any():
            df_display[col_name] = df_lots[key].apply(lambda x: dashboard_utils.format_number(x, config.PRICE_DISPLAY_PRECISION) if pd.notna(x) and x > 0 else '-')
            final_cols.append(col_name)
            
    if 'risk_usd' in df_lots and df_lots['risk_usd'].notna().any():
        col_name = t('col_risk_usd')
        df_display[col_name] = df_lots['risk_usd'].apply(lambda x: dashboard_utils.format_number(-abs(x), currency_symbol=display_currency) if pd.notna(x) else '-')
        final_cols.append(col_name)

    df_display[t('col_notes')] = df_lots['notes'].fillna('')
    final_cols.append(t('col_notes'))
    
    # --- ПРИМЕНЕНИЕ ЦВЕТОВЫХ СТИЛЕЙ ---
    styler = df_display[final_cols].style
    
    styles_to_apply = {
        style_pnl: [t('col_pnl_sum')],
        style_sl: [t('col_sl')],
        style_tp: [t('col_tp1'), t('col_tp2'), t('col_tp3')],
        style_risk: [t('col_risk_usd')]
    }

    for style_func, cols in styles_to_apply.items():
        existing_cols = [col for col in cols if col in df_display.columns]
        if existing_cols:
            styler = styler.applymap(style_func, subset=existing_cols)
            
    st.dataframe(styler, hide_index=True, use_container_width=True)


def display_closed_trades(df: pd.DataFrame, core_trades: List[TradeData], selected_exchanges: list, selected_symbols: list):
    st.markdown(f"### {t('closed_trades_header')}")
    if df.empty: st.info(t('no_closed_deals_data')); return
    
    trade_id_to_exchange = {trade.trade_id: trade.exchange for trade in core_trades if trade.trade_id}
    df['exchange'] = df['sell_trade_id'].map(trade_id_to_exchange)
    
    if selected_exchanges: df = df[df['exchange'].isin(selected_exchanges)]
    if selected_symbols: df = df[df['symbol'].isin(selected_symbols)]
    if df.empty: st.info(t('no_closed_deals_after_filter')); return
    
    df = df.sort_values(by='timestamp_closed', ascending=False)
    
    if 'num_trades_to_show' not in st.session_state: st.session_state.num_trades_to_show = 10
    df_to_display = df.head(st.session_state.num_trades_to_show)

    df_display = pd.DataFrame()
    df_display[t('col_symbol')] = df_to_display['symbol']; df_display[t('col_exchange')] = df_to_display['exchange'].fillna('-'); df_display[t('col_timestamp_closed')] = pd.to_datetime(df_to_display['timestamp_closed']).dt.strftime('%Y-%m-%d %H:%M'); df_display[t('col_qty')] = df_to_display['matched_qty'].apply(lambda x: dashboard_utils.format_number(x, config.QTY_DISPLAY_PRECISION)); df_display[t('col_buy_price')] = df_to_display['buy_price'].apply(lambda x: dashboard_utils.format_number(x, config.PRICE_DISPLAY_PRECISION)); df_display[t('col_sell_price')] = df_to_display['sell_price'].apply(lambda x: dashboard_utils.format_number(x, config.PRICE_DISPLAY_PRECISION)); df_display[t('col_pnl_fifo')] = df_to_display['fifo_pnl'].apply(lambda x: dashboard_utils.format_number(Decimal(x), add_plus_sign=True, currency_symbol=display_currency))
    
    # --- ПРИМЕНЕНИЕ ЦВЕТОВЫХ СТИЛЕЙ ДЛЯ PNL ---
    st.dataframe(df_display.style.applymap(style_pnl, subset=[t('col_pnl_fifo')]), hide_index=True, use_container_width=True)

    if len(df) > st.session_state.num_trades_to_show:
        if st.button(t('show_more_button')):
            st.session_state.num_trades_to_show += 10
            st.rerun()

# --- ГЛАВНЫЙ КОД ---
all_data, all_errors = dashboard_utils.load_all_data_with_error_handling()
if all_errors:
    with st.expander("⚠️ Обнаружены ошибки", expanded=True):
        for msg in all_errors:
            st.error(f"- {msg}")

positions_data = all_data.get('open_positions', [])
closed_trades_data = all_data.get('fifo_logs', [])
analytics_data = all_data.get('analytics_history', [])
core_trades_data = all_data.get('core_trades', [])
account_balances = all_data.get('account_balances', [])

core_trades_df = pd.DataFrame([t.__dict__ for t in core_trades_data]) if core_trades_data else pd.DataFrame(columns=['symbol', 'exchange'])
closed_trades_df = pd.DataFrame([t.__dict__ for t in closed_trades_data]) if closed_trades_data else pd.DataFrame(columns=['symbol', 'exchange'])
selected_exchanges, selected_symbols = setup_filters(core_trades_df, closed_trades_df)

current_prices = dashboard_utils.fetch_current_prices_for_all_exchanges(positions_data)

positions_df = pd.DataFrame([p.__dict__ for p in positions_data]) if positions_data else pd.DataFrame(columns=['symbol', 'exchange'])
total_crypto_value = Decimal('0')
total_unrealized_pnl = Decimal('0')
if not positions_df.empty:
    def get_price(row): return current_prices.get(str(row.get('exchange', '')).lower(), {}).get(row.get('symbol'), Decimal('0'))
    positions_df['current_price'] = positions_df.apply(get_price, axis=1)
    for col in ['net_amount', 'avg_entry_price', 'current_price']:
        positions_df[col] = positions_df[col].apply(Decimal)
    positions_df['current_value'] = positions_df['net_amount'] * positions_df['current_price']
    positions_df['unrealized_pnl'] = (positions_df['current_price'] - positions_df['avg_entry_price']) * positions_df['net_amount']
    total_crypto_value = positions_df['current_value'].sum()
    total_unrealized_pnl = positions_df['unrealized_pnl'].sum()

total_stables_value = sum(Decimal(b.balance) for b in account_balances if b.asset in config.INVESTMENT_ASSETS and b.balance is not None)
live_total_equity = total_crypto_value + total_stables_value

# --- Переключатель периода и расчет дельты ---
col1, col2 = st.columns([1, 4])
with col1:
    # [ИСПРАВЛЕНО] Определяем опции с ключами для логики
    # и значениями из локализации для отображения.
    period_options = {
        'day': t('period_day'),
        'month': t('period_month'),
        'year': t('period_year')
    }
    # st.radio теперь возвращает ключ ('day', 'month', 'year'),
    # а пользователю показывает переведенное название.
    time_period_key = st.radio(
        label=t('period_selector_label'),
        options=list(period_options.keys()),
        format_func=lambda key: period_options[key],
        horizontal=True,
        key='delta_period'
    )

equity_delta_str = None
if analytics_data:
    now = datetime.now()
    # Логика теперь работает с надежными английскими ключами
    if time_period_key == 'day':
        target_date = now - timedelta(days=1)
    elif time_period_key == 'month':
        target_date = now - timedelta(days=30)
    else: # 'year'
        target_date = now - timedelta(days=365)

    closest_record = min(
        analytics_data, 
        key=lambda x: abs(pd.to_datetime(x.date_generated).replace(tzinfo=None) - target_date.replace(tzinfo=None))
    )
    
    previous_equity = Decimal(closest_record.total_equity)
    
    if previous_equity > 0:
        delta_percent = ((live_total_equity / previous_equity) - 1) * 100
        
        # Используем ключ для перевода строки формата
        # Получаем переведенное название периода для отображения
        translated_period = period_options[time_period_key]
        # Получаем строку формата из перевода
        format_string = t('delta_format_string') # Пример в locales/ru.json: "delta_format_string": "{value:+.2f}% / {period}"
        # Собираем финальную строку
        equity_delta_str = format_string.format(value=delta_percent, period=translated_period.lower())

latest_analytics_obj = analytics_data[-1] if analytics_data else None
if latest_analytics_obj:
    display_capital_overview(latest_analytics_obj, live_total_equity, total_unrealized_pnl, equity_delta_str)
else:
    st.info(t('no_data_for_analytics'))

st.divider()
display_open_lots_table(core_trades_data, current_prices, selected_exchanges, selected_symbols)
st.divider()
display_closed_trades(closed_trades_df.copy(), core_trades_data, selected_exchanges, selected_symbols)
