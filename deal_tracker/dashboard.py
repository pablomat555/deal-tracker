
# deal_tracker/dashboard.py
from locales import t
import config
import dashboard_utils
import streamlit as st
import pandas as pd
import logging
import os
import sys
from decimal import Decimal, InvalidOperation

# Добавляем корень проекта в путь для корректных импортов
project_root = os.path.abspath(os.path.dirname(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# --- НАСТРОЙКА СТРАНИЦЫ И ЛОГГЕР ---
st.set_page_config(layout="wide", page_title=t('app_title'))
logger = logging.getLogger(__name__)

# --- ПЕРЕКЛЮЧАТЕЛЬ ЯЗЫКА ---
with st.sidebar:
    lang = st.radio("🌐 Язык / Language",
                    options=["ru", "en"], index=0 if st.session_state.get("lang") == "ru" else 1)
    st.session_state["lang"] = lang

# --- ФУНКЦИИ ОТОБРАЖЕНИЯ ---


def format_colored_pnl(value):
    color = "green" if value >= 0 else "red"
    return f"<span style='color:{color};'>{dashboard_utils.format_number(value, add_plus_sign=True)}</span>"


def display_capital_overview(latest_analytics):
    if not latest_analytics:
        st.info(t('no_data_for_analytics'))
        return

    st.markdown(f"### {t('capital_overview_header')}")
    col1, col2, col3, col4 = st.columns(4)

    col1.metric(t('total_equity'), dashboard_utils.format_number(
        latest_analytics.total_equity, currency_symbol=config.BASE_CURRENCY))
    col2.metric(t('net_invested'), dashboard_utils.format_number(
        latest_analytics.net_invested_funds, currency_symbol=config.BASE_CURRENCY))

    net_pnl = latest_analytics.net_total_pnl
    realized_pnl = latest_analytics.total_realized_pnl
    unrealized_pnl = latest_analytics.total_unrealized_pnl

    col3.metric(t('total_pnl'),
                dashboard_utils.format_number(
                    net_pnl, add_plus_sign=True, currency_symbol=config.BASE_CURRENCY),
                delta=f"{net_pnl:+.2f}")

    with col4.container(border=True):
        st.markdown(
            f"<small>{t('realized_pnl')}: <strong>{format_colored_pnl(realized_pnl)}</strong></small>", unsafe_allow_html=True)
        st.markdown(
            f"<small>{t('unrealized_pnl')}: <strong>{format_colored_pnl(unrealized_pnl)}</strong></small>", unsafe_allow_html=True)

    st.caption(
        f"{t('data_from')} {latest_analytics.date_generated.strftime('%Y-%m-%d %H:%M:%S')}")


def display_active_investments(positions_data):
    st.markdown(f"### {t('investments_header')}")
    if not positions_data:
        st.info(t('no_open_positions'))
        return

    def to_decimal_safe(value):
        if value is None:
            return Decimal('0')
        try:
            return Decimal(value)
        except (TypeError, InvalidOperation):
            return Decimal('0')

    processed_positions = []
    for pos in positions_data:
        net_amount = to_decimal_safe(pos.net_amount)
        avg_entry_price = to_decimal_safe(pos.avg_entry_price)
        current_price = to_decimal_safe(pos.current_price)
        unrealized_pnl = to_decimal_safe(pos.unrealized_pnl)
        position_value = net_amount * current_price

        processed_positions.append({
            t('col_symbol'): pos.symbol,
            t('col_exchange'): pos.exchange,
            t('col_qty'): float(net_amount),
            t('col_avg_entry'): float(avg_entry_price),
            t('current_price'): float(current_price),
            t('col_price'): float(pos.execution_price or 0),
            t('col_value'): float(position_value),
            t('col_share_percent'): f"{pos.share_percent:.2f}%" if pos.share_percent else "0.00%",
            t('current_pnl'): format_colored_pnl(unrealized_pnl)
        })

    df = pd.DataFrame(processed_positions)
    st.markdown(df.to_html(escape=False, index=False), unsafe_allow_html=True)


# --- ГЛАВНЫЙ КОД ---
st.title(t('app_title'))
if st.button(t('update_button')):
    st.experimental_rerun()

# Загрузка данных
latest_analytics = dashboard_utils.fetch_latest_analytics()
positions_data = dashboard_utils.fetch_positions()

# Отображение
display_capital_overview(latest_analytics)
display_active_investments(positions_data)
