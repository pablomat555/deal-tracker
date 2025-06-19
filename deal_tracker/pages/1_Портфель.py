# pages/1_Портфель.py
import streamlit as st
import pandas as pd
from decimal import Decimal
import plotly.express as px
import logging

# ИСПРАВЛЕНО: Правильные импорты
import dashboard_utils
import config
from locales import t

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
    # --- СОЗДАНИЕ ДЕТАЛЬНОГО DF ПОРТФЕЛЯ ---
    portfolio_components = []
    # Добавляем криптоактивы из открытых позиций
    for pos in open_positions:
        if pos.net_amount and pos.current_price and pos.net_amount > 0 and pos.current_price > 0:
            portfolio_components.append({
                'category': t('category_crypto'),
                'asset': pos.symbol.split('/')[0],
                'value_usd': pos.net_amount * pos.current_price,
                'location': pos.exchange
            })
    # Добавляем свободные средства (стейблкоины)
    for balance in account_balances:
        if balance.asset in INVESTMENT_ASSETS and balance.balance > 0:
            portfolio_components.append({
                'category': t('category_stables'),
                'asset': balance.asset,
                'value_usd': balance.balance,
                'location': balance.account_name
            })

    df_portfolio = pd.DataFrame(portfolio_components)
    total_portfolio_value = df_portfolio.['value_usd'].sum()

    st.metric(t('total_selected_assets_value'), dashboard_utils.format_number(
        total_portfolio_value, currency_symbol=config.BASE_CURRENCY))
    st.divider()

    # --- ВИЗУАЛИЗАЦИЯ ---
    col1, col2 = st.columns([1, 1])
    with col1:
        st.subheader(t('capital_structure_header'))
        df_structure = df_portfolio.groupby(
            'category')['value_usd'].sum().reset_index()
        if not df_structure.empty and df_structure['value_usd'].sum() > 0:
            fig = px.pie(df_structure, values='value_usd', names='category', title=t(
                'capital_distribution_chart_title'))
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader(t('asset_allocation_header'))
        df_allocation = df_portfolio.groupby(
            'asset')['value_usd'].sum().reset_index()
        if not df_allocation.empty and df_allocation['value_usd'].sum() > 0:
            fig_alloc = px.pie(df_allocation, values='value_usd', names='asset', title=t(
                'asset_distribution_chart_title'))
            fig_alloc.update_traces(
                textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_alloc, use_container_width=True)

    st.divider()
    # --- ДЕТАЛЬНАЯ ТАБЛИЦА ---
    st.subheader(t('portfolio_details_subheader'))
    if not df_portfolio.empty:
        df_portfolio['share_%'] = (
            df_portfolio['value_usd'] / total_portfolio_value * 100) if total_portfolio_value > 0 else 0
        df_portfolio_display = pd.DataFrame()
        df_portfolio_display[t('col_asset_source')] = df_portfolio['asset'] + \
            " (" + df_portfolio['location'] + ")"
        df_portfolio_display[t('col_value_currency')] = df_portfolio['value_usd'].map(
            dashboard_utils.format_number)
        df_portfolio_display[t('col_share_percent')] = df_portfolio['share_%'].map(
            lambda x: f"{dashboard_utils.format_number(x, '0.01')}%")

        st.dataframe(df_portfolio_display.sort_values(by=t('col_value_currency'), ascending=False),
                     use_container_width=True, hide_index=True)
