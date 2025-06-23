# deal_tracker/dashboard.py
from locales import t
import config
import dashboard_utils
import streamlit as st
import pandas as pd
import logging
import os
import sys

# Добавляем корень проекта в путь для корректных импортов
project_root = os.path.abspath(os.path.dirname(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Новые, правильные импорты

# --- НАСТРОЙКА СТРАНИЦЫ И ЛОГГЕР ---
st.set_page_config(layout="wide", page_title=t('app_title'))
logger = logging.getLogger(__name__)

# --- ФУНКЦИИ ОТОБРАЖЕНИЯ (АДАПТИРОВАНЫ ПОД МОДЕЛИ) ---


def display_capital_overview(latest_analytics):
    """Отображает верхний блок с ключевыми метриками капитала."""
    if not latest_analytics:
        st.info(t('no_data_for_analytics'))
        return

    st.markdown(f"### {t('capital_overview_header')}")
    col1, col2, col3 = st.columns(3)

    # Прямой доступ к атрибутам модели
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


def display_active_investments(positions_data):
    """Отображает секцию с активными инвестициями (ранее 'display_active_investments_section')."""
    st.markdown(f"### {t('investments_header')}")
    if not positions_data:
        st.info(t('no_open_positions'))
        return

    # Pandas отлично создает DataFrame из списка объектов dataclass
    df = pd.DataFrame([p.__dict__ for p in positions_data])

    # Вычисления напрямую с типизированными колонками
    df['Current_Value'] = df['net_amount'] * df['current_price']
    total_value = df['Current_Value'].sum()
    df['Share_%'] = (df['Current_Value'] / total_value *
                     100) if total_value > 0 else 0

    # Создаем DataFrame для отображения с форматированием
    df_display = pd.DataFrame({
        t('col_symbol'): df['symbol'],
        t('col_exchange'): df['exchange'],
        t('col_qty'): df['net_amount'].map(lambda x: dashboard_utils.format_number(x, config.QTY_DISPLAY_PRECISION)),
        t('col_avg_entry'): df['avg_entry_price'].map(lambda x: dashboard_utils.format_number(x, config.PRICE_DISPLAY_PRECISION)),
        t('col_price'): df['current_price'].map(lambda x: dashboard_utils.format_number(x, config.PRICE_DISPLAY_PRECISION)),
        t('col_value'): df['Current_Value'].map(lambda x: dashboard_utils.format_number(x, config.USD_DISPLAY_PRECISION)),
        t('col_share'): df['Share_%'].map(lambda x: f"{dashboard_utils.format_number(x, '0.01')}%"),
        t('col_pnl_sum'): df['unrealized_pnl'].map(lambda x: dashboard_utils.format_number(x, add_plus_sign=True)),
    })

    styler = df_display.style.map(
        dashboard_utils.style_pnl_value, subset=[t('col_pnl_sum')])
    st.dataframe(styler, hide_index=True, use_container_width=True)


def display_recent_trades(trades_data):
    """Отображает экспандер с последними сделками."""
    with st.expander(t('core_trades_header')):
        if not trades_data:
            st.info(t('no_core_records'))
            return

        df = pd.DataFrame([t.__dict__ for t in trades_data])
        df_sorted = df.sort_values(by="timestamp", ascending=False).head(
            15)  # Показываем последние 15

        df_display = pd.DataFrame({
            t('col_date'): df_sorted['timestamp'].dt.strftime('%Y-%m-%d %H:%M'),
            t('col_symbol'): df_sorted['symbol'],
            t('col_type'): df_sorted['trade_type'],
            t('col_amount'): df_sorted['amount'].map(lambda x: dashboard_utils.format_number(x, config.QTY_DISPLAY_PRECISION)),
            t('col_price'): df_sorted['price'].map(lambda x: dashboard_utils.format_number(x, config.PRICE_DISPLAY_PRECISION)),
            t('col_exchange'): df_sorted['exchange'],
        })
        st.dataframe(df_display, hide_index=True, use_container_width=True)


# --- ОСНОВНАЯ ЧАСТЬ ДЭШБОРДА ---
if st.button(t('update_button'), key="main_refresh_dashboard"):
    st.cache_data.clear()
    st.rerun()

all_data = dashboard_utils.load_all_dashboard_data()
analytics_history = all_data.get('analytics_history', [])
latest_analytics = analytics_history[-1] if analytics_history else None

# --- БЛОК ФИЛЬТРОВ (теперь работает с объектами) ---
st.markdown("---")
col1, col2 = st.columns(2)

# Собираем уникальные значения из списков объектов
all_exchanges = sorted(
    list(set(p.exchange for p in all_data.get('open_positions', []))))
all_symbols = sorted(
    list(set(p.symbol for p in all_data.get('open_positions', []))))

selected_exchanges = col1.multiselect(
    t('filter_by_exchange'), options=all_exchanges, default=st.session_state.get('exch_filter', []))
selected_symbols = col2.multiselect(
    t('filter_by_asset'), options=all_symbols, default=st.session_state.get('sym_filter', []))

# Сохраняем состояние фильтров
st.session_state['exch_filter'] = selected_exchanges
st.session_state['sym_filter'] = selected_symbols

# --- ФИЛЬТРАЦИЯ ДАННЫХ ---
positions_to_display = all_data.get('open_positions', [])
trades_to_display = all_data.get('core_trades', [])

if selected_exchanges:
    positions_to_display = [
        p for p in positions_to_display if p.exchange in selected_exchanges]
    trades_to_display = [
        t for t in trades_to_display if t.exchange in selected_exchanges]

if selected_symbols:
    positions_to_display = [
        p for p in positions_to_display if p.symbol in selected_symbols]
    trades_to_display = [
        t for t in trades_to_display if t.symbol in selected_symbols]

# --- Отображение всех блоков ---
display_capital_overview(latest_analytics)
st.divider()
display_active_investments(positions_to_display)
st.divider()
# Место для display_trading_results_section (будет на другой странице)
display_recent_trades(trades_to_display)
