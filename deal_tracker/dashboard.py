# deal_tracker/dashboard.py
# [ИЗМЕНЕНО] Убедимся, что используем наш обновленный dashboard_utils
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
st.set_page_config(layout="wide", page_title=t('app_title'))
logger = logging.getLogger(__name__)

# --- ПЕРЕКЛЮЧАТЕЛЬ ЯЗЫКА ---
with st.sidebar:
    lang_options = ["ru", "en"]
    current_lang = st.session_state.get("lang", "ru")
    lang_index = lang_options.index(
        current_lang) if current_lang in lang_options else 0
    lang = st.radio("🌐 Язык / Language", options=lang_options,
                    index=lang_index, key='lang_radio')
    st.session_state["lang"] = lang

# --- ФУНКЦИИ ОТОБРАЖЕНИЯ ---


def display_capital_overview(latest_analytics: dict):
    """Отображает блок с общей аналитикой капитала."""
    if not latest_analytics:
        st.info(t('no_data_for_analytics'))
        return

    st.markdown(f"### {t('capital_overview_header')}")
    col1, col2, col3, col4 = st.columns(4)

    # Используем .get() для безопасного доступа и safe_to_decimal для преобразования
    total_equity = dashboard_utils.safe_to_decimal(
        latest_analytics.get('total_equity'))
    net_invested = dashboard_utils.safe_to_decimal(
        latest_analytics.get('net_invested_funds'))
    net_pnl = dashboard_utils.safe_to_decimal(
        latest_analytics.get('net_total_pnl'))
    realized_pnl = dashboard_utils.safe_to_decimal(
        latest_analytics.get('total_realized_pnl'))
    # Нереализованный PNL теперь будем считать из актуальных данных
    unrealized_pnl = total_equity - net_invested - realized_pnl

    col1.metric(t('total_equity'), dashboard_utils.format_number(
        total_equity, currency_symbol=config.BASE_CURRENCY))
    col2.metric(t('net_invested'), dashboard_utils.format_number(
        net_invested, currency_symbol=config.BASE_CURRENCY))
    col3.metric(t('total_pnl'), dashboard_utils.format_number(
        net_pnl, add_plus_sign=True, currency_symbol=config.BASE_CURRENCY), delta=f"{net_pnl:+.2f}")

    with col4.container(border=True):
        st.markdown(
            f"<small>{t('realized_pnl')}: <strong>{dashboard_utils.format_number(realized_pnl, add_plus_sign=True)}</strong></small>", unsafe_allow_html=True)
        st.markdown(
            f"<small>{t('unrealized_pnl')}: <strong>{dashboard_utils.format_number(unrealized_pnl, add_plus_sign=True)}</strong></small>", unsafe_allow_html=True)

    date_generated_str = latest_analytics.get('date_generated', '')
    st.caption(f"{t('data_from')} {date_generated_str}")


def display_active_investments(positions_data: list, current_prices: dict):
    """
    [ПЕРЕРАБОТАНО] Отображает таблицу активных инвестиций с актуальными ценами и PNL.
    """
    st.markdown(f"### {t('investments_header')}")
    if not positions_data:
        st.info(t('no_open_positions'))
        return

    # Преобразуем список объектов/словарей в DataFrame
    try:
        df = pd.DataFrame([pos.__dict__ if hasattr(
            pos, '__dict__') else pos for pos in positions_data])
    except Exception as e:
        st.error(f"Ошибка при создании DataFrame: {e}")
        return

    # 1. Применяем актуальные цены
    def get_price(row):
        exchange_id = str(row.get('Exchange', '')).lower()
        symbol = row.get('Symbol')
        return current_prices.get(exchange_id, {}).get(symbol, Decimal('0'))

    df['Current_Price'] = df.apply(get_price, axis=1)

    # 2. Безопасно конвертируем колонки в Decimal
    for col in ['Net_Amount', 'Avg_Entry_Price', 'Current_Price']:
        df[col] = df[col].apply(dashboard_utils.safe_to_decimal)

    # 3. Рассчитываем метрики с использованием векторизации pandas
    df['Current_Value'] = df['Net_Amount'] * df['Current_Price']
    df['Unrealized_PNL'] = (df['Current_Price'] -
                            df['Avg_Entry_Price']) * df['Net_Amount']

    total_value = df['Current_Value'].sum()
    df['Share'] = (df['Current_Value'] / total_value *
                   100) if total_value > 0 else 0

    # 4. Форматируем колонки для отображения
    df_display = pd.DataFrame()
    df_display[t('col_symbol')] = df['Symbol']
    df_display[t('col_exchange')] = df['Exchange']
    df_display[t('col_qty')] = df['Net_Amount'].apply(
        lambda x: dashboard_utils.format_number(x, precision_str='0.00001'))
    df_display[t('col_avg_entry')] = df['Avg_Entry_Price'].apply(
        lambda x: dashboard_utils.format_number(x, precision_str='0.0001'))
    df_display[t('col_price')] = df['Current_Price'].apply(
        lambda x: dashboard_utils.format_number(x, precision_str='0.0001'))
    df_display[t('col_value')] = df['Current_Value'].apply(
        lambda x: dashboard_utils.format_number(x, currency_symbol=config.BASE_CURRENCY))
    df_display[t('col_share_percent')] = df['Share'].apply(
        lambda x: f"{dashboard_utils.format_number(x)}%")
    df_display[t('current_pnl')] = df['Unrealized_PNL'].apply(
        lambda x: dashboard_utils.format_number(x, add_plus_sign=True))

    # 5. Отображаем через st.dataframe со стилизацией
    st.dataframe(
        df_display.style.applymap(
            dashboard_utils.style_pnl_value, subset=[t('current_pnl')]),
        hide_index=True,
        use_container_width=True
    )


# --- ГЛАВНЫЙ КОД ---
st.title(t('app_title'))
if st.button(t('update_button')):
    st.cache_data.clear()
    st.rerun()

# [ИЗМЕНЕНО] Блок загрузки и обработки данных
# 1. Загружаем статические данные из Google Sheets
all_data = dashboard_utils.load_all_dashboard_data()
analytics_history = all_data.get('analytics_history', [])
latest_analytics = analytics_history[-1] if analytics_history else {}
positions_data = all_data.get('open_positions', [])

# 2. [НОВЫЙ БЛОК] Получаем актуальные рыночные цены для этих позиций
current_prices = dashboard_utils.fetch_current_prices_for_all_exchanges(
    positions_data)

# 3. Отображаем все блоки с актуальными данными
display_capital_overview(latest_analytics)
st.divider()
display_active_investments(positions_data, current_prices)
