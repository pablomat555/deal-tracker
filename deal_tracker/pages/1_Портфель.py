# pages/1_Портфель.py
from locales import t
import config
import dashboard_utils
import streamlit as st
import pandas as pd
import plotly.express as px
import logging
import os
import sys

# Добавляем корень проекта в путь
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.append(project_root)


# --- НАСТРОЙКИ И ЗАГРУЗКА ДАННЫХ ---
st.set_page_config(layout="wide", page_title=t('page_portfolio_title'))
st.title(t('page_portfolio_header'))
if st.button(t('refresh_page_button'), key="portfolio_refresh"):
    st.cache_data.clear()
    st.rerun()

all_data = dashboard_utils.load_all_dashboard_data()
open_positions = all_data.get('open_positions', [])
account_balances = all_data.get('account_balances', [])
INVESTMENT_ASSETS = getattr(config, 'INVESTMENT_ASSETS', ['USDT', 'USDC'])

# --- ОСНОВНАЯ ЛОГИКА ---
if not open_positions and not any(b.asset in INVESTMENT_ASSETS for b in account_balances):
    st.info(t('no_portfolio_data'))
else:
    portfolio_components = []
    for pos in open_positions:
        # ИСПРАВЛЕНО: Проверка на None для всех числовых атрибутов
        if pos.net_amount and pos.current_price and pos.net_amount > 0 and pos.current_price > 0:
            portfolio_components.append({
                'category': t('category_crypto'),
                'asset': pos.symbol.split('/')[0],
                'value_usd': pos.net_amount * pos.current_price,
                'location': pos.exchange
            })
    for balance in account_balances:
        if balance.asset in INVESTMENT_ASSETS and balance.balance and balance.balance > 0:
            portfolio_components.append({
                'category': t('category_stables'),
                'asset': balance.asset,
                'value_usd': balance.balance,
                'location': balance.account_name
            })

    df_portfolio = pd.DataFrame(portfolio_components)

    # ИСПРАВЛЕНО: Убрана лишняя точка
    total_portfolio_value = df_portfolio['value_usd'].sum(
    ) if not df_portfolio.empty else 0

    st.metric(t('total_selected_assets_value'), dashboard_utils.format_number(
        total_portfolio_value, currency_symbol=config.BASE_CURRENCY))
    st.divider()

    # --- ВИЗУАЛИЗАЦИЯ И ТАБЛИЦА ---
    # ... (здесь ваш код для графиков и детальной таблицы, он должен работать с df_portfolio) ...
