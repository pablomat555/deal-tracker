# deal_tracker/dashboard.py
import streamlit as st
import pandas as pd
import logging

# ИСПРАВЛЕНО: Правильные импорты из новых утилит
import dashboard_utils
import config
from locales import t

# --- НАСТРОЙКА СТРАНИЦЫ И ЛОГГЕР ---
st.set_page_config(layout="wide", page_title=t('app_title'))
st.markdown("<style>/* ... CSS ... */</style>", unsafe_allow_html=True)
st.sidebar.radio("Язык/Language", ['ru', 'en'],
                 format_func=lambda x: "Русский" if x == 'ru' else "English", key='lang')
logger = logging.getLogger(__name__)

# --- ФУНКЦИЯ ОТОБРАЖЕНИЯ ---


def display_capital_overview(latest_analytics):
    if not latest_analytics:
        st.info(t('no_data_for_analytics'))
        return

    st.markdown(f"### {t('capital_overview_header')}")
    col1, col2, col3 = st.columns(3)

    # ИСПРАВЛЕНО: Прямой доступ к атрибутам модели
    col1.metric(t('total_equity'), dashboard_utils.format_number(
        latest_analytics.total_equity, currency_symbol=config.BASE_CURRENCY))
    col2.metric(t('net_invested'), dashboard_utils.format_number(
        latest_analytics.net_invested_funds, currency_symbol=config.BASE_CURRENCY))
    col3.metric(t('total_pnl'),
                dashboard_utils.format_number(
                    latest_analytics.net_total_pnl, add_plus_sign=True, currency_symbol=config.BASE_CURRENCY),
                delta=f"{latest_analytics.net_total_pnl:+.2f}")
    st.caption(
        f"{t('data_from')} {latest_analytics.date_generated.strftime('%Y-%m-%d %H:%M:%S')}")


# --- ОСНОВНАЯ ЧАСТЬ ---
if st.button(t('update_button'), key="main_refresh_dashboard"):
    st.cache_data.clear()
    st.rerun()

all_data = dashboard_utils.load_all_dashboard_data()
analytics_history = all_data.get('analytics_history', [])
latest_analytics = analytics_history[-1] if analytics_history else None

display_capital_overview(latest_analytics)

st.info("Выберите раздел в меню слева для просмотра деталей.")
logger.info("Отрисовка главной страницы дэшборда завершена.")
