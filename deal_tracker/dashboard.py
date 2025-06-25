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
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# --- НАСТРОЙКА СТРАНИЦЫ И ЛОГГЕР ---
st.set_page_config(layout="wide", page_title=t('app_title'))
logger = logging.getLogger(__name__)

# --- ПЕРЕКЛЮЧАТЕЛЬ ЯЗЫКА ---
with st.sidebar:
    lang_options = ["ru", "en"]
    current_lang = st.session_state.get("lang", "ru")
    lang_index = lang_options.index(
        current_lang) if current_lang in lang_options else 0
    lang = st.radio("🌐 Язык / Language",
                    options=lang_options, index=lang_index)
    st.session_state["lang"] = lang

# --- УНИВЕРСАЛЬНЫЕ ФУНКЦИИ ФОРМАТИРОВАНИЯ ---


def format_colored_pnl(val: any) -> str:
    """
    Форматирует PnL в HTML-строку с цветом, используя утилиты из dashboard_utils.
    """
    style = dashboard_utils.style_pnl_value(val)
    formatted_number = dashboard_utils.format_number(val, add_plus_sign=True)
    return f"<span style='{style}'>{formatted_number}</span>"

# --- ФУНКЦИИ ОТОБРАЖЕНИЯ ---


def display_capital_overview(latest_analytics: dashboard_utils.AnalyticsData):
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
    col3.metric(t('total_pnl'),
                dashboard_utils.format_number(
                    net_pnl, add_plus_sign=True, currency_symbol=config.BASE_CURRENCY),
                delta=f"{net_pnl:+.2f}")

    with col4.container(border=True):
        st.markdown(
            f"<small>{t('realized_pnl')}: <strong>{format_colored_pnl(latest_analytics.total_realized_pnl)}</strong></small>", unsafe_allow_html=True)
        st.markdown(
            f"<small>{t('unrealized_pnl')}: <strong>{format_colored_pnl(latest_analytics.total_unrealized_pnl)}</strong></small>", unsafe_allow_html=True)

    st.caption(
        f"{t('data_from')} {latest_analytics.date_generated.strftime('%Y-%m-%d %H:%M:%S')}")


def display_active_investments(positions_data: list):
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
        current_price = to_decimal_safe(pos.current_price)
        position_value = net_amount * current_price

        processed_positions.append({
            t('col_symbol'): pos.symbol,
            t('col_exchange'): pos.exchange,
            t('col_qty'): float(net_amount),
            t('col_avg_entry'): float(to_decimal_safe(pos.avg_entry_price)),
            t('current_price'): float(current_price),
            t('col_value'): float(position_value),
            t('col_share_percent'): f"{pos.share_percent:.2f}%" if pos.share_percent else "0.00%",
            t('current_pnl'): format_colored_pnl(to_decimal_safe(pos.unrealized_pnl))
        })

    df = pd.DataFrame(processed_positions)
    # Используем to_html для рендера кастомного HTML в ячейках
    st.markdown(df.to_html(escape=False, index=False,
                justify="center"), unsafe_allow_html=True)


# --- ГЛАВНЫЙ КОД ---
st.title(t('app_title'))
if st.button(t('update_button')):
    # Очищаем кэш данных принудительно и перезапускаем страницу
    st.cache_data.clear()
    st.rerun()

# Централизованная загрузка данных
all_data = dashboard_utils.load_all_dashboard_data()
latest_analytics = all_data.get(
    'analytics_history', [])[-1] if all_data.get('analytics_history') else None
positions_data = all_data.get('open_positions', [])


# Отображение
display_capital_overview(latest_analytics)
display_active_investments(positions_data)
