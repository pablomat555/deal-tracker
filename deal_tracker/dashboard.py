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


def display_capital_overview(latest_analytics: dict, unrealized_pnl_from_positions: Decimal):
    if not latest_analytics:
        st.info(t('no_data_for_analytics'))
        return

    st.markdown(f"### {t('capital_overview_header')}")
    col1, col2, col3, col4 = st.columns(4)

    total_equity = Decimal(latest_analytics.total_equity)
    net_invested = Decimal(latest_analytics.net_invested_funds)
    realized_pnl = Decimal(latest_analytics.total_realized_pnl)
    net_pnl = realized_pnl + unrealized_pnl_from_positions

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
            f"<small>{t('unrealized_pnl')}: <strong>{dashboard_utils.format_number(unrealized_pnl_from_positions, add_plus_sign=True)}</strong></small>", unsafe_allow_html=True)

    st.caption(
        f"{t('data_from')} {latest_analytics.date_generated.strftime('%Y-%m-%d %H:%M:%S')}")


def display_active_investments(positions_data: list, current_prices: dict) -> Decimal:
    st.markdown(f"### {t('investments_header')}")
    if not positions_data:
        st.info(t('no_open_positions'))
        return Decimal('0')

    # Преобразуем список объектов PositionData в DataFrame
    df = pd.DataFrame([pos.__dict__ for pos in positions_data])

    # 1. Применяем актуальные цены
    def get_price(row):
        # [ИСПРАВЛЕНО] Ключи словаря теперь соответствуют атрибутам модели (маленькие буквы)
        exchange_id = str(row.get('exchange', '')).lower()
        symbol = row.get('symbol')
        return current_prices.get(exchange_id, {}).get(symbol, Decimal('0'))

    df['current_price'] = df.apply(get_price, axis=1)

    # 2. Безопасно конвертируем колонки в Decimal
    for col in ['net_amount', 'avg_entry_price', 'current_price']:
        df[col] = df[col].apply(Decimal)

    # 3. Рассчитываем метрики
    df['current_value'] = df['net_amount'] * df['current_price']
    df['unrealized_pnl'] = (df['current_price'] -
                            df['avg_entry_price']) * df['net_amount']

    total_portfolio_value = df['current_value'].sum()
    df['share'] = (df['current_value'] / total_portfolio_value *
                   100) if total_portfolio_value > 0 else 0

    # 4. Форматируем колонки для отображения
    df_display = pd.DataFrame()
    df_display[t('col_symbol')] = df['symbol']
    df_display[t('col_exchange')] = df['exchange']
    df_display[t('col_qty')] = df['net_amount'].apply(
        lambda x: dashboard_utils.format_number(x, precision_str='0.00001'))
    df_display[t('col_avg_entry')] = df['avg_entry_price'].apply(
        lambda x: dashboard_utils.format_number(x, precision_str='0.0001'))
    df_display[t('current_price')] = df['current_price'].apply(
        lambda x: dashboard_utils.format_number(x, precision_str='0.0001'))
    df_display[t('col_value')] = df['current_value'].apply(
        lambda x: dashboard_utils.format_number(x, currency_symbol=config.BASE_CURRENCY))
    df_display[t('col_share_percent')] = df['share'].apply(
        lambda x: f"{dashboard_utils.format_number(x)}%")
    df_display[t('current_pnl')] = df['unrealized_pnl'].apply(
        lambda x: dashboard_utils.format_number(x, add_plus_sign=True))

    # 5. Отображаем через st.dataframe со стилизацией
    st.dataframe(
        df_display.style.applymap(
            dashboard_utils.style_pnl_value, subset=[t('current_pnl')]),
        hide_index=True,
        use_container_width=True
    )

    # Возвращаем общую сумму нереализованного PNL для верхнего блока
    return df['unrealized_pnl'].sum()


# --- ГЛАВНЫЙ КОД ---
st.title(t('app_title'))
if st.button(t('update_button')):
    st.cache_data.clear()
    st.rerun()

# 1. Загружаем статические данные
all_data = dashboard_utils.load_all_dashboard_data()
analytics_history = all_data.get('analytics_history', [])
latest_analytics_obj = analytics_history[-1] if analytics_history else None
positions_data = all_data.get('open_positions', [])

# 2. Получаем актуальные рыночные цены
current_prices = dashboard_utils.fetch_current_prices_for_all_exchanges(
    positions_data)

# 3. Отображаем все блоки
total_unrealized_pnl = display_active_investments(
    positions_data, current_prices)
st.divider()
if latest_analytics_obj:
    display_capital_overview(latest_analytics_obj, total_unrealized_pnl)
else:
    st.info(t('no_data_for_analytics'))
