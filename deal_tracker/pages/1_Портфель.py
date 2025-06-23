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
core_trades = all_data.get('core_trades', [])
INVESTMENT_ASSETS = getattr(config, 'INVESTMENT_ASSETS', ['USDT', 'USDC'])

# --- БЛОК ФИЛЬТРОВ (теперь работает с объектами) ---
st.markdown("---")
col1, col2 = st.columns(2)

all_exchanges = sorted(list(set(b.account_name for b in account_balances)))
all_symbols = sorted(list(set(p.symbol for p in open_positions)
                     | set(t.symbol for t in core_trades)))

selected_exchanges = col1.multiselect(
    t('filter_by_exchange'), options=all_exchanges)
selected_symbols = col2.multiselect(t('filter_by_asset'), options=all_symbols)

# --- ФИЛЬТРАЦИЯ ДАННЫХ (теперь работает с объектами) ---
filtered_open_positions = open_positions
filtered_account_balances = account_balances
selected_assets_from_symbols = {s.split('/')[0] for s in selected_symbols}

if selected_exchanges:
    filtered_open_positions = [
        p for p in filtered_open_positions if p.exchange in selected_exchanges]
    filtered_account_balances = [
        b for b in filtered_account_balances if b.account_name in selected_exchanges]

if selected_symbols:
    filtered_open_positions = [
        p for p in filtered_open_positions if p.symbol in selected_symbols]
    # Фильтруем балансы: оставляем стейблы И базовые активы выбранных торговых пар
    filtered_account_balances = [
        b for b in filtered_account_balances if b.asset in selected_assets_from_symbols or b.asset in INVESTMENT_ASSETS]

# --- БЛОК СВОДКИ ПО СТЕЙБЛКОИНАМ ---
st.subheader(t('stablecoin_summary_header'))
stablecoin_balances = [
    b for b in filtered_account_balances if b.asset in INVESTMENT_ASSETS]

if stablecoin_balances:
    df_stables = pd.DataFrame([b.__dict__ for b in stablecoin_balances])
    total_stables_value = df_stables['balance'].sum()
    st.metric(f"{t('total_stablecoins')}", dashboard_utils.format_number(
        total_stables_value, currency_symbol=config.BASE_CURRENCY))
    # ... (здесь ваш код для детализации по стейблкоинам, он должен работать) ...
else:
    st.info(t('no_stablecoin_data'))
st.divider()

# --- БЛОК ДЕТАЛИЗАЦИИ ПОРТФЕЛЯ ---
st.header(t('portfolio_details_header'))
portfolio_components = []

for pos in filtered_open_positions:
    if pos.net_amount and pos.current_price and pos.net_amount > 0:
        value_usd = pos.net_amount * pos.current_price
        if value_usd > 0:
            portfolio_components.append({
                'ТипКомпонента': 'Криптоактив',
                'Актив_или_Счет': pos.symbol.split('/')[0],
                'Стоимость_USD': value_usd,
                'Биржа_Счет': pos.exchange
            })

for balance in stablecoin_balances:
    if balance.balance and balance.balance > 0:
        portfolio_components.append({
            'ТипКомпонента': 'СвободныеСредства',
            'Актив_или_Счет': balance.asset,
            'Стоимость_USD': balance.balance,
            'Биржа_Счет': balance.account_name
        })

if not portfolio_components:
    st.info(t('no_portfolio_data'))
else:
    df_portfolio_full = pd.DataFrame(portfolio_components)
    final_total_value = df_portfolio_full['Стоимость_USD'].sum()

    st.metric(t('total_selected_assets_value'), dashboard_utils.format_number(
        final_total_value, currency_symbol=config.BASE_CURRENCY))

    st.subheader(t('capital_structure_header'))
    df_portfolio_full['Категория'] = df_portfolio_full['ТипКомпонента'].replace(
        {'Криптоактив': t('category_crypto'), 'СвободныеСредства': t('category_stables')})
    df_capital_structure = df_portfolio_full.groupby(
        'Категория')['Стоимость_USD'].sum().reset_index()

    if not df_capital_structure.empty and df_capital_structure['Стоимость_USD'].sum() > 0:
        df_capital_structure['Formatted_Value'] = df_capital_structure['Стоимость_USD'].apply(
            lambda x: dashboard_utils.format_number(x, currency_symbol=config.BASE_CURRENCY))
        fig_structure = px.pie(df_capital_structure, values='Стоимость_USD', names='Категория', title=t(
            'capital_distribution_chart_title'), custom_data=['Formatted_Value'])
        fig_structure.update_traces(textposition='inside', textinfo='percent+label',
                                    hovertemplate='<b>%{label}</b><br>Стоимость: %{customdata[0]}<br>Доля: %{percent}')
        st.plotly_chart(fig_structure, use_container_width=True)

    st.subheader(t('portfolio_details_subheader'))
    df_table = df_portfolio_full.groupby(['Биржа_Счет', 'Актив_или_Счет'])[
        'Стоимость_USD'].sum().reset_index()
    if final_total_value > 0:
        df_table['Доля, %'] = (
            df_table['Стоимость_USD'] / final_total_value) * 100
    else:
        df_table['Доля, %'] = Decimal('0')

    df_table[t('col_value_currency')] = df_table['Стоимость_USD'].apply(
        dashboard_utils.format_number)
    df_table[t('col_share_percent')] = df_table['Доля, %'].apply(
        lambda x: f"{dashboard_utils.format_number(x, '0.01')}%")
    df_table[t('col_asset_source')] = df_table['Актив_или_Счет'] + \
        " (" + df_table['Биржа_Счет'] + ")"
    df_sorted_for_display = df_table.sort_values(
        by='Стоимость_USD', ascending=False)

    st.dataframe(
        df_sorted_for_display[[t('col_asset_source'), t(
            'col_value_currency'), t('col_share_percent')]],
        use_container_width=True, hide_index=True
    )
