# pages/1_Портфель.py
from locales import t
import config
import dashboard_utils
import streamlit as st
import pandas as pd
from decimal import Decimal, InvalidOperation
import plotly.express as px
import os
import sys

# Добавляем корень проекта в путь для корректных импортов
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Импортируем наши рабочие модули

# --- НАСТРОЙКА СТРАНИЦЫ И ЗАГРУЗКА ДАННЫХ ---
st.set_page_config(layout="wide", page_title=t('page_portfolio_title'))
st.title(t('page_portfolio_header'))


@st.cache_data(ttl=300)
def load_data():
    """Загружает только необходимые для этой страницы данные."""
    return {
        'open_positions': dashboard_utils.sheets_service.get_all_open_positions(),
        'account_balances': dashboard_utils.sheets_service.get_all_balances()
    }


data = load_data()
open_positions = data.get('open_positions', [])
account_balances = data.get('account_balances', [])
INVESTMENT_ASSETS = getattr(config, 'INVESTMENT_ASSETS', ['USDT', 'USDC'])

# --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ БЕЗОПАСНОЙ КОНВЕРТАЦИИ ---


def to_decimal_safe(value):
    """Преобразует любое значение в Decimal, возвращая 0 при любой ошибке."""
    if value is None:
        return Decimal('0')
    try:
        # Универсальная обработка: удаляем пробелы, меняем запятые
        return Decimal(str(value).replace(',', '.').strip())
    except (TypeError, InvalidOperation):
        return Decimal('0')


# --- ОСНОВНАЯ ЛОГИКА ОТОБРАЖЕНИЯ ---
if not open_positions and not any(b.asset in INVESTMENT_ASSETS for b in account_balances):
    st.info(t('no_portfolio_data'))
else:
    # --- СОЗДАНИЕ ДЕТАЛЬНОГО DF ПОРТФЕЛЯ ---
    portfolio_components = []

    # 1. Обработка открытых позиций
    for pos in open_positions:
        # Принудительно конвертируем каждое значение перед использованием
        net_amount = to_decimal_safe(pos.net_amount)
        current_price = to_decimal_safe(pos.current_price)

        if net_amount > 0 and current_price > 0:
            value_usd = net_amount * current_price
            portfolio_components.append({
                'category': t('category_crypto'),
                'asset': pos.symbol.split('/')[0],
                'value_usd': value_usd,
                'location': pos.exchange
            })

    # 2. Обработка стейблкоинов
    for balance in account_balances:
        balance_dec = to_decimal_safe(balance.balance)
        if balance.asset in INVESTMENT_ASSETS and balance_dec > 0:
            portfolio_components.append({
                'category': t('category_stables'),
                'asset': balance.asset,
                'value_usd': balance_dec,
                'location': balance.account_name
            })

    if not portfolio_components:
        st.info(t('no_portfolio_data_after_processing'))
    else:
        df_portfolio = pd.DataFrame(portfolio_components)
        total_portfolio_value = df_portfolio['value_usd'].sum()

        st.metric(t('total_selected_assets_value'), dashboard_utils.format_number(
            total_portfolio_value, currency_symbol=config.BASE_CURRENCY))
        st.divider()

        # --- ВИЗУАЛИЗАЦИЯ (без изменений, теперь должна работать) ---
        col1, col2 = st.columns([1, 1])
        with col1:
            st.markdown(f"#### {t('asset_allocation_header')}")
            fig_assets = px.pie(df_portfolio, names='asset', values='value_usd', title=t(
                'asset_distribution_title'), hole=0.4)
            fig_assets.update_traces(textinfo='percent+label')
            st.plotly_chart(fig_assets, use_container_width=True)
        with col2:
            st.markdown(f"#### {t('structure_by_location_header')}")
            fig_locations = px.pie(df_portfolio, names='location', values='value_usd', title=t(
                'location_distribution_title'), hole=0.4)
            fig_locations.update_traces(textinfo='percent+label')
            st.plotly_chart(fig_locations, use_container_width=True)
