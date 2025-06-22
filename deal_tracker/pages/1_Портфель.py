# pages/1_Портфель.py

# --- НАЧАЛО УНИВЕРСАЛЬНОГО БЛОКА ---
from deal_tracker.locales import t
from deal_tracker import config
from deal_tracker import dashboard_utils
import logging
import plotly.express as px
import pandas as pd
import streamlit as st
import sys
import os

# 1. Добавляем корневую папку проекта в системный путь
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 2. Теперь импорты делаем явными, от имени главного пакета
# Явные импорты из пакета deal_tracker
# --- КОНЕЦ УНИВЕРСАЛЬНОГО БЛОКА ---


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
        # Проверка на None для всех числовых атрибутов
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

    if not portfolio_components:
        st.info(t('no_portfolio_data'))
    else:
        df_portfolio = pd.DataFrame(portfolio_components)

        total_portfolio_value = df_portfolio['value_usd'].sum()

        st.metric(t('total_selected_assets_value'), dashboard_utils.format_number(
            total_portfolio_value, currency_symbol=config.BASE_CURRENCY))
        st.divider()

        # --- ВИЗУАЛИЗАЦИЯ И ТАБЛИЦА ---
        col1, col2 = st.columns([0.4, 0.6])

        with col1:
            st.markdown(f"#### {t('structure_by_asset_header')}")
            fig_assets = px.pie(
                df_portfolio,
                names='asset',
                values='value_usd',
                title=t('asset_distribution_title'),
                hole=0.4
            )
            fig_assets.update_traces(textinfo='percent+label', pull=[
                                     0.05]*len(df_portfolio['asset'].unique()))
            st.plotly_chart(fig_assets, use_container_width=True)

        with col2:
            st.markdown(f"#### {t('structure_by_location_header')}")
            fig_locations = px.pie(
                df_portfolio,
                names='location',
                values='value_usd',
                title=t('location_distribution_title'),
                hole=0.4
            )
            fig_locations.update_traces(textinfo='percent+label', pull=[
                                        0.05]*len(df_portfolio['location'].unique()))
            st.plotly_chart(fig_locations, use_container_width=True)

        st.divider()
        st.markdown(f"### {t('detailed_portfolio_header')}")
        st.dataframe(
            df_portfolio.sort_values(by='value_usd', ascending=False),
            use_container_width=True,
            hide_index=True,
            column_config={
                "category": t('col_category'),
                "asset": t('col_asset'),
                "value_usd": st.column_config.NumberColumn(t('col_value_usd'), format=f"{config.BASE_CURRENCY} %.2f"),
                "location": t('col_location')
            }
        )
