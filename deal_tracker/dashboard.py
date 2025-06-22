# deal_tracker/dashboard.py

# --- НАЧАЛО УНИВЕРСАЛЬНОГО БЛОКА ---
from deal_tracker.locales import t
from deal_tracker import config
from deal_tracker import dashboard_utils
import logging
import streamlit as st
import sys
import os

# 1. Добавляем корневую папку проекта в системный путь
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 2. Теперь импорты делаем явными, от имени главного пакета
# Используем явные импорты от пакета deal_tracker
# --- КОНЕЦ УНИВЕРСАЛЬНОГО БЛОКА ---


# --- НАСТРОЙКА СТРАНИЦЫ И ЛОГГЕР ---
st.set_page_config(layout="wide", page_title=t('app_title'))
logger = logging.getLogger(__name__)


# --- ФУНКЦИЯ ОТОБРАЖЕНИЯ ---
def display_capital_overview(latest_analytics):
    if not latest_analytics:
        st.info(t('no_data_for_analytics'))
        return

    st.markdown(f"### {t('capital_overview_header')}")
    col1, col2, col3 = st.columns(3)

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
    # Очищаем кэш и перезапускаем страницу для обновления данных
    st.cache_data.clear()
    st.rerun()

all_data = dashboard_utils.load_all_dashboard_data()
analytics_history = all_data.get('analytics_history', [])
latest_analytics = analytics_history[-1] if analytics_history else None

display_capital_overview(latest_analytics)

st.info("Выберите раздел в меню слева для просмотра деталей.")
logger.info("Отрисовка главной страницы дэшборда завершена.")
