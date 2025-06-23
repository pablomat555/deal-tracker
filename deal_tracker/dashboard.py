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


# --- ФУНКЦИИ ОТОБРАЖЕНИЯ (ФИНАЛЬНАЯ ВЕРСИЯ) ---

def display_capital_overview(latest_analytics):
    """Отображает верхний блок с ключевыми метриками капитала."""
    if not latest_analytics:
        st.info(t('no_data_for_analytics'))
        return

    st.markdown(f"### {t('capital_overview_header')}")
    # ИСПРАВЛЕНО: Теперь 4 колонки для детального PNL
    col1, col2, col3, col4 = st.columns(4)

    # Прямой доступ к атрибутам модели
    col1.metric(t('total_equity'), dashboard_utils.format_number(
        latest_analytics.total_equity, currency_symbol=config.BASE_CURRENCY))
    col2.metric(t('net_invested'), dashboard_utils.format_number(
        latest_analytics.net_invested_funds, currency_symbol=config.BASE_CURRENCY))

    # Детализация PNL
    realized_pnl = latest_analytics.total_realized_pnl
    unrealized_pnl = latest_analytics.total_unrealized_pnl
    net_pnl = latest_analytics.net_total_pnl

    col3.metric(t('total_pnl'),
                dashboard_utils.format_number(
                    net_pnl, add_plus_sign=True, currency_symbol=config.BASE_CURRENCY),
                delta=f"{net_pnl:+.2f}")

    with col4.container(border=True):
        st.markdown(
            f"<small>{t('realized_pnl')}: **{dashboard_utils.format_number(realized_pnl, add_plus_sign=True)}**</small>", unsafe_allow_html=True)
        st.markdown(
            f"<small>{t('unrealized_pnl')}: **{dashboard_utils.format_number(unrealized_pnl, add_plus_sign=True)}**</small>", unsafe_allow_html=True)

    st.caption(
        f"{t('data_from')} {latest_analytics.date_generated.strftime('%Y-%m-%d %H:%M:%S')}")


def display_active_investments(positions_data):
    """Отображает секцию с активными инвестициями."""
    st.markdown(f"### {t('investments_header')}")
    if not positions_data:
        st.info(t('no_open_positions'))
        return

    # Вспомогательная функция для безопасного преобразования
    def to_decimal_safe(value):
        if value is None:
            return Decimal('0')
        try:
            return Decimal(value)
        except (TypeError, InvalidOperation):
            return Decimal('0')

    # Готовим данные
    processed_positions = []
    for pos in positions_data:
        net_amount = to_decimal_safe(pos.net_amount)
        current_price = to_decimal_safe(pos.current_price)
        avg_entry_price = to_decimal_safe(pos.avg_entry_price)
        unrealized_pnl = to_decimal_safe(pos.unrealized_pnl)

        current_value = net_amount * current_price

        processed_positions.append({
            'symbol': pos.symbol,
            'exchange': pos.exchange,
            'net_amount': net_amount,
            'avg_entry_price': avg_entry_price,
            'current_price': current_price,
            'current_value': current_value,
            'unrealized_pnl': unrealized_pnl
        })

    if not processed_positions:
        st.info(t('no_open_positions'))
        return

    df = pd.DataFrame(processed_positions)
    total_value = df['current_value'].sum()
    df['share_%'] = (df['current_value'] / total_value *
                     100) if total_value > 0 else 0

    # Создаем DataFrame для отображения
    df_display = pd.DataFrame({
        t('col_symbol'): df['symbol'],
        t('col_exchange'): df['exchange'],
        t('col_qty'): df['net_amount'],
        t('col_avg_entry'): df['avg_entry_price'],
        t('col_price'): df['current_price'],
        # ИСПРАВЛЕНО: Добавлена колонка стоимости
        t('col_value'): df['current_value'],
        t('col_share'): df['share_%'],
        t('col_pnl_sum'): df['unrealized_pnl'],
    })

    # Применяем форматирование и стили
    styler = df_display.style.format({
        t('col_qty'): "{:.4f}",
        t('col_avg_entry'): "{:.4f}",
        t('col_price'): "{:.4f}",
        t('col_value'): "{:,.2f} $",
        t('col_share'): "{:.2f}%",
        t('col_pnl_sum'): "{:+.2f} $"
    }).map(dashboard_utils.style_pnl_value, subset=[t('col_pnl_sum')])

    st.dataframe(styler, hide_index=True, use_container_width=True)


def display_recent_trades(trades_data):
    """Отображает экспандер с последними сделками."""
    with st.expander(t('core_trades_header')):
        if not trades_data:
            st.info(t('no_core_records'))
            return

        df = pd.DataFrame([t.__dict__ for t in trades_data])
        df_sorted = df.sort_values(by="timestamp", ascending=False).head(15)

        df_display = pd.DataFrame({
            t('col_date'): pd.to_datetime(df_sorted['timestamp']).dt.strftime('%Y-%m-%d %H:%M'),
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

# --- БЛОК ФИЛЬТРОВ ---
st.markdown("---")
col1, col2 = st.columns(2)

all_exchanges = sorted(
    list(set(p.exchange for p in all_data.get('open_positions', []))))
all_symbols = sorted(
    list(set(p.symbol for p in all_data.get('open_positions', []))))

selected_exchanges = col1.multiselect(
    t('filter_by_exchange'), options=all_exchanges, default=st.session_state.get('exch_filter', []))
selected_symbols = col2.multiselect(
    t('filter_by_asset'), options=all_symbols, default=st.session_state.get('sym_filter', []))

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
display_recent_trades(trades_to_display)
