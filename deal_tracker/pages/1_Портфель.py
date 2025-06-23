# pages/1_Портфель.py
from locales import t
import dashboard_utils
import config
import logging
import plotly.express as px
import pandas as pd
import streamlit as st
import os
import sys
from decimal import Decimal, InvalidOperation

# Добавляем корень проекта в путь.
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Используем простые, прямые импорты


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

    # Вспомогательная функция для безопасного преобразования в Decimal
    def to_decimal_safe(value):
        if value is None:
            return Decimal('0')
        try:
            return Decimal(value)
        except (TypeError, InvalidOperation):
            # Попробуем очистить строку, если прямое преобразование не удалось
            try:
                cleaned_val = ''.join(c for c in str(
                    value) if c in '0123456789.-')
                return Decimal(cleaned_val) if cleaned_val else Decimal('0')
            except (TypeError, InvalidOperation):
                return Decimal('0')

    # ИСПРАВЛЕНО: Безопасный цикл для сбора данных
    for pos in open_positions:
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

        # --- ВИЗУАЛИЗАЦИЯ ---
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

        # Переименовываем колонки перед отображением для соответствия переводам
        df_display = df_portfolio.rename(columns={
            'category': t('col_category'),
            'asset': t('col_asset'),
            'value_usd': t('col_value_usd'),
            'location': t('col_location')
        })

        st.dataframe(
            df_display.sort_values(by=t('col_value_usd'), ascending=False),
            use_container_width=True,
            hide_index=True,
            column_config={
                t('col_value_usd'): st.column_config.NumberColumn(format=f"{config.BASE_CURRENCY} %.2f"),
            }
        )
