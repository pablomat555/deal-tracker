# pages/1_Портфель.py
from locales import t
import config
import dashboard_utils
import streamlit as st
import pandas as pd
from decimal import Decimal
import plotly.express as px
import logging
import os
import sys

# Добавляем корень проекта в путь для корректных импортов
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Новые, правильные импорты

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

    # --- ИСПРАВЛЕНО: Безопасный цикл для сбора данных ---
    for pos in open_positions:
        # Безопасно преобразуем значения, заменяя None на 0
        net_amount = pos.net_amount if pos.net_amount is not None else Decimal(
            '0')
        current_price = pos.current_price if pos.current_price is not None else Decimal(
            '0')

        if net_amount > 0 and current_price > 0:
            value_usd = net_amount * current_price
            portfolio_components.append({
                'category': t('category_crypto'),
                'asset': pos.symbol.split('/')[0],
                'value_usd': value_usd,
                'location': pos.exchange
            })

    for balance in account_balances:
        balance_dec = balance.balance if balance.balance is not None else Decimal(
            '0')
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

        # --- ВИЗУАЛИЗАЦИЯ (без изменений) ---
        col1, col2 = st.columns([1, 1])
        with col1:
            st.markdown(f"#### {t('asset_allocation_header')}")
            fig_assets = px.pie(
                df_portfolio, names='asset', values='value_usd',
                title=t('asset_distribution_title'), hole=0.4
            )
            fig_assets.update_traces(textinfo='percent+label')
            st.plotly_chart(fig_assets, use_container_width=True)

        with col2:
            st.markdown(f"#### {t('structure_by_location_header')}")
            fig_locations = px.pie(
                df_portfolio, names='location', values='value_usd',
                title=t('location_distribution_title'), hole=0.4
            )
            fig_locations.update_traces(textinfo='percent+label')
            st.plotly_chart(fig_locations, use_container_width=True)

        st.divider()
        st.markdown(f"### {t('detailed_portfolio_header')}")
        df_portfolio['share'] = (
            df_portfolio['value_usd'] / total_portfolio_value * 100) if total_portfolio_value > 0 else 0
        df_display = pd.DataFrame({
            t('col_asset'): df_portfolio['asset'],
            t('col_location'): df_portfolio['location'],
            t('col_category'): df_portfolio['category'],
            t('col_value_usd'): df_portfolio['value_usd'].map(lambda x: dashboard_utils.format_number(x)),
            t('col_share_percent'): df_portfolio['share'].map(lambda x: f"{dashboard_utils.format_number(x, '0.01')}%")
        })
        st.dataframe(df_display.sort_values(by=t('col_value_usd'),
                     ascending=False), use_container_width=True, hide_index=True)
