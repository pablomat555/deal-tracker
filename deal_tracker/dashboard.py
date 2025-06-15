# dashboard.py (Главная страница)
import streamlit as st
import pandas as pd
from decimal import Decimal
import logging

from utils import (
    load_all_dashboard_data, safe_to_decimal, format_number,
    get_first_buy_trade_details, style_pnl_value
)
import config
from locales import t

# --- НАСТРОЙКА СТРАНИЦЫ, ШРИФТА И ЯЗЫКА ---
st.set_page_config(layout="wide", page_title=t('app_title'))
st.markdown(
    """<style>@import url('https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700&display=swap'); html, body, [class*="st-"], [class*="css-"] {font-family: 'Roboto', sans-serif;}</style>""", unsafe_allow_html=True)
st.sidebar.radio("Язык/Language", options=['ru', 'en'],
                 format_func=lambda x: "Русский" if x == 'ru' else "English", key='lang')

# --- ЛОГГЕР ---
_logger = logging.getLogger(__name__)
if not _logger.handlers:
    _handler = logging.StreamHandler()
    _formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    _handler.setFormatter(_formatter)
    _logger.addHandler(_handler)
    _logger.setLevel(logging.INFO)

# --- ФУНКЦИИ ОТОБРАЖЕНИЯ БЛОКОВ ---


def display_capital_overview(latest_analytics: dict):
    """Отображает верхний блок с ключевыми метриками капитала."""
    if latest_analytics:
        col_eq1, col_eq2, col_eq3 = st.columns(3)
        total_equity = safe_to_decimal(
            latest_analytics.get('Total_Equity', '0'))
        net_invested = safe_to_decimal(
            latest_analytics.get('Net_Invested_Funds', '0'))
        net_pnl = safe_to_decimal(latest_analytics.get('Net_Total_PNL', '0'))

        col_eq1.metric(t('total_equity'), format_number(
            total_equity, show_currency_symbol=config.BASE_CURRENCY))
        col_eq2.metric(t('net_invested'), format_number(
            net_invested, show_currency_symbol=config.BASE_CURRENCY))

        # ИСПРАВЛЕНО: Удален параметр delta_color для автоматического выбора цвета
        col_eq3.metric(t('total_pnl'),
                       format_number(net_pnl, add_plus_sign=True,
                                     show_currency_symbol=config.BASE_CURRENCY),
                       delta=format_number(net_pnl, add_plus_sign=True))
        st.caption(
            f"{t('data_from')} {latest_analytics.get('Date_Generated', 'N/A')}")
    else:
        st.info(t('no_data_for_analytics'))


def display_active_investments_section(open_positions_data: list):
    """Отображает секцию с активными инвестициями."""
    st.markdown(f"### {t('investments_header')}")
    if not open_positions_data:
        st.info(t('no_open_positions'))
        return

    df_positions = pd.DataFrame(open_positions_data)

    df_positions['Net_Amount_dec'] = df_positions['Net_Amount'].apply(
        safe_to_decimal)
    df_positions['Avg_Entry_Price_dec'] = df_positions['Avg_Entry_Price'].apply(
        safe_to_decimal)
    df_positions['Current_Price_dec'] = df_positions['Current_Price'].apply(
        safe_to_decimal)
    df_positions['Current_Value_calc'] = df_positions['Net_Amount_dec'] * \
        df_positions['Current_Price_dec']

    pnl_mask = df_positions['Avg_Entry_Price_dec'] != 0
    df_positions.loc[pnl_mask, 'PNL_%_calc'] = (
        (df_positions['Current_Price_dec'] / df_positions['Avg_Entry_Price_dec']) - 1) * 100
    df_positions.loc[pnl_mask, 'PNL_$_calc'] = (
        df_positions['Current_Price_dec'] - df_positions['Avg_Entry_Price_dec']) * df_positions['Net_Amount_dec']
    df_positions.fillna({'PNL_%_calc': 0, 'PNL_$_calc': 0}, inplace=True)

    total_value = df_positions['Current_Value_calc'].sum()
    if total_value > 0:
        df_positions['Share_%_calc'] = (
            df_positions['Current_Value_calc'] / total_value) * 100
    else:
        df_positions['Share_%_calc'] = 0

    df_positions[t('col_qty')] = df_positions['Net_Amount_dec'].apply(
        lambda x: format_number(x, '0.00001'))
    df_positions[t('col_avg_entry')] = df_positions['Avg_Entry_Price_dec'].apply(
        lambda x: format_number(x, '0.00001'))
    df_positions[t('col_price')] = df_positions['Current_Price_dec'].apply(
        lambda x: format_number(x, '0.00001'))
    df_positions[t('col_value')] = df_positions['Current_Value_calc'].apply(
        format_number)
    df_positions[t('col_share')] = df_positions['Share_%_calc'].apply(
        lambda x: f"{format_number(x, '0.01')}%")
    df_positions[t('col_pnl_percent')] = df_positions['PNL_%_calc'].apply(
        lambda x: f"{format_number(x, '0.01', add_plus_sign=True)}%")
    df_positions[t('col_pnl_sum')] = df_positions['PNL_$_calc'].apply(
        lambda x: format_number(x, add_plus_sign=True))

    df_final = df_positions.rename(
        columns={'Symbol': t('col_symbol'), 'Exchange': t('col_exchange')})

    cols_to_show = [t('col_symbol'), t('col_exchange'), t('col_qty'), t('col_avg_entry'), t(
        'col_price'), t('col_value'), t('col_share'), t('col_pnl_percent'), t('col_pnl_sum')]

    # ИСПРАВЛЕНО: Устаревший .applymap() заменен на .map()
    styler = df_final[cols_to_show].style.map(
        style_pnl_value, subset=[t('col_pnl_percent'), t('col_pnl_sum')])
    st.dataframe(styler, hide_index=True, use_container_width=True)


def display_trading_results_section(latest_analytics: dict, fifo_logs_data: list):
    """Отображает секцию с результатами закрытых сделок."""
    st.markdown(f"### {t('trading_results_header')}")
    if latest_analytics:
        col_res1, col_res2, col_res3, col_res4 = st.columns(4)
        realized_pnl = safe_to_decimal(
            latest_analytics.get('Total_Realized_PNL', '0'))
        win_rate = safe_to_decimal(
            latest_analytics.get('Win_Rate_Percent', '0'))
        profit_factor = str(latest_analytics.get(
            'Profit_Factor', 'N/A')).strip()
        total_trades = int(safe_to_decimal(
            latest_analytics.get('Total_Trades_Closed', '0')))

        # ИСПРАВЛЕНО: Удален параметр delta_color для автоматического выбора цвета
        col_res1.metric(t('realized_pnl'), format_number(realized_pnl, add_plus_sign=True, show_currency_symbol=config.BASE_CURRENCY),
                        delta=format_number(realized_pnl, add_plus_sign=True))
        col_res2.metric(t('win_rate'), f"{format_number(win_rate, '0.01')}%")
        col_res3.metric(t('profit_factor'), "∞" if profit_factor.lower() in [
                        'inf', 'infinity'] else profit_factor)
        col_res4.metric(t('total_closed_trades'), str(total_trades))

    st.markdown(f"#### {t('fifo_details_header')}")
    if not fifo_logs_data:
        st.info(t('no_closed_deals_after_filter'))
        return

    df_fifo = pd.DataFrame(fifo_logs_data)
    df_sorted = df_fifo.sort_values(by="Timestamp_Closed", ascending=False)

    df_sorted[t('col_qty')] = df_sorted['Matched_Qty'].apply(
        lambda x: format_number(x, '0.00000'))
    df_sorted[t('col_buy_price')] = df_sorted['Buy_Price'].apply(
        lambda x: format_number(x, '0.0001'))
    df_sorted[t('col_sell_price')] = df_sorted['Sell_Price'].apply(
        lambda x: format_number(x, '0.0001'))
    df_sorted[t('col_pnl_fifo')] = df_sorted['Fifo_PNL'].apply(
        lambda x: format_number(x, add_plus_sign=True))

    df_final = df_sorted.rename(columns={'Timestamp_Closed': t('col_close_time'), 'Symbol': t(
        'col_symbol'), 'Buy_Trade_ID': t('col_buy_id'), 'Sell_Trade_ID': t('col_sell_id'), 'Exchange': t('col_exchange')})

    cols_to_show = [t('col_close_time'), t('col_symbol'), t('col_exchange'), t('col_qty'), t(
        'col_buy_price'), t('col_sell_price'), t('col_pnl_fifo'), t('col_buy_id'), t('col_sell_id')]
    final_cols = [col for col in cols_to_show if col in df_final.columns]

    # ИСПРАВЛЕНО: Устаревший .applymap() заменен на .map()
    styler = df_final[final_cols].style.map(
        style_pnl_value, subset=[t('col_pnl_fifo')])
    st.dataframe(styler, hide_index=True, use_container_width=True)


def display_recent_core_trades_section(core_trades_data: list):
    """Отображает экспандер с последними базовыми сделками."""
    with st.expander(t('core_trades_header')):
        if not core_trades_data:
            st.info(t('no_core_records'))
            return

        df_core = pd.DataFrame(core_trades_data)
        df_sorted = df_core.sort_values(by="Timestamp", ascending=False)

        df_sorted[t('col_price')] = df_sorted['Price'].apply(
            lambda x: format_number(x, '0.0001'))
        df_sorted[t('col_amount')] = df_sorted['Amount'].apply(
            lambda x: format_number(x, '0.00000'))
        df_sorted[t('col_commission')] = df_sorted['Commission'].apply(
            lambda x: format_number(x, '0.000001'))
        df_sorted[t('col_trade_pnl')] = df_sorted['Trade_PNL'].apply(
            lambda x: format_number(x, add_plus_sign=True))

        df_final = df_sorted.rename(columns={
            'Timestamp': t('col_date'),
            'Order_ID': t('col_order_id'),
            'Exchange': t('col_exchange'),
            'Symbol': t('col_symbol'),
            'Type': t('col_type'),
            'Commission_Asset': t('col_fee_asset'),
            'Notes': t('col_notes')
        })

        cols_to_show = [t('col_date'), t('col_order_id'), t('col_exchange'), t('col_symbol'), t('col_type'), t(
            'col_amount'), t('col_price'), t('col_commission'), t('col_fee_asset'), t('col_trade_pnl'), t('col_notes')]
        final_cols = [col for col in cols_to_show if col in df_final.columns]

        styler = df_final[final_cols].style
        if t('col_trade_pnl') in final_cols:
            # ИСПРАВЛЕНО: Устаревший .applymap() заменен на .map()
            styler = styler.map(
                style_pnl_value, subset=[t('col_trade_pnl')])

        st.dataframe(styler, hide_index=True, use_container_width=True)


# --- ОСНОВНАЯ ЧАСТЬ ДЭШБОРДА ---
if st.button(t('update_button'), key="main_refresh_dashboard"):
    st.cache_data.clear()
    st.rerun()

all_data = load_all_dashboard_data()

analytics_history_data = all_data.get('analytics_history', [])
latest_analytics = analytics_history_data[-1] if analytics_history_data else {}

# --- БЛОК ФИЛЬТРОВ ---
st.markdown("---")
col1, col2 = st.columns(2)

# Собираем уникальные значения для фильтров
open_positions_df = pd.DataFrame(all_data.get('open_positions', []))
core_trades_df = pd.DataFrame(all_data.get('core_trades', []))
all_exchanges = []
if not open_positions_df.empty and 'Exchange' in open_positions_df.columns:
    all_exchanges.extend(open_positions_df['Exchange'].unique())
if not core_trades_df.empty and 'Exchange' in core_trades_df.columns:
    all_exchanges.extend(core_trades_df['Exchange'].unique())
all_exchanges = sorted(list(set(all_exchanges)))

all_symbols = []
if not open_positions_df.empty and 'Symbol' in open_positions_df.columns:
    all_symbols.extend(open_positions_df['Symbol'].unique())
if not core_trades_df.empty and 'Symbol' in core_trades_df.columns:
    all_symbols.extend(core_trades_df['Symbol'].unique())
all_symbols = sorted(list(set(all_symbols)))

# Отображаем виджеты
selected_exchanges = col1.multiselect(
    t('filter_by_exchange'), options=all_exchanges)
selected_symbols = col2.multiselect(
    t('filter_by_asset'), options=all_symbols)

# --- ФИЛЬТРАЦИЯ ДАННЫХ ---
filtered_positions = all_data.get('open_positions', [])
filtered_fifo_logs = all_data.get('fifo_logs', [])
filtered_core_trades = all_data.get('core_trades', [])

if selected_exchanges:
    filtered_positions = [p for p in filtered_positions if p.get(
        'Exchange') in selected_exchanges]
    filtered_fifo_logs = [log for log in filtered_fifo_logs if log.get(
        'Exchange') in selected_exchanges]
    filtered_core_trades = [trade for trade in filtered_core_trades if trade.get(
        'Exchange') in selected_exchanges]

if selected_symbols:
    filtered_positions = [p for p in filtered_positions if p.get(
        'Symbol') in selected_symbols]
    filtered_fifo_logs = [log for log in filtered_fifo_logs if log.get(
        'Symbol') in selected_symbols]
    filtered_core_trades = [trade for trade in filtered_core_trades if trade.get(
        'Symbol') in selected_symbols]


# --- Отображение всех блоков с отфильтрованными данными ---
display_capital_overview(latest_analytics)
st.divider()
display_active_investments_section(filtered_positions)
st.divider()
display_trading_results_section(latest_analytics, filtered_fifo_logs)
st.divider()
display_recent_core_trades_section(filtered_core_trades)

_logger.info("Отрисовка главной страницы дэшборда (dashboard.py) завершена.")
