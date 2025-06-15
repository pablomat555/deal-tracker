# pages/1_Портфель.py
import streamlit as st
import pandas as pd
from decimal import Decimal
import logging
import plotly.express as px
import os
import sys

try:
    from utils import safe_to_decimal, format_number, load_all_dashboard_data
    from locales import t
    import config
except ImportError:
    project_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.append(project_root)
    from utils import safe_to_decimal, format_number, load_all_dashboard_data
    from locales import t
    import config

# --- НАСТРОЙКИ ---
BASE_CURRENCY = getattr(config, 'BASE_CURRENCY', 'USD').upper()
INVESTMENT_ASSETS = getattr(config, 'INVESTMENT_ASSETS', [
                            'USD', 'USDT', 'USDC', 'DAI', 'BUSD'])

# --- НАСТРОЙКА СТРАНИЦЫ ---
st.set_page_config(layout="wide", page_title=t('page_portfolio_title'))
st.markdown(
    """<style>@import url('https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700&display=swap'); html, body, [class*="st-"], [class*="css-"] {font-family: 'Roboto', sans-serif;}</style>""", unsafe_allow_html=True)
st.sidebar.radio("Язык/Language", options=['ru', 'en'],
                 format_func=lambda x: "Русский" if x == 'ru' else "English", key='lang')

# --- ЗАГОЛОВОК И КНОПКА ОБНОВЛЕНИЯ ---
st.title(t('page_portfolio_header'))
if st.button(t('refresh_page_button'), key="portfolio_refresh"):
    st.cache_data.clear()
    st.rerun()

all_data = load_all_dashboard_data()
open_positions_data = all_data.get('open_positions', [])
account_balances_data = all_data.get('account_balances', [])
core_trades_data = all_data.get('core_trades', [])

# --- БЛОК ФИЛЬТРОВ ---
st.markdown("---")
col1, col2 = st.columns(2)

all_exchanges = sorted(list(set(
    [b.get('Account_Name')
     for b in account_balances_data if b.get('Account_Name')]
)))
all_symbols = sorted(list(set(
    [p.get('Symbol') for p in open_positions_data if p.get('Symbol')] +
    [t.get('Symbol') for t in core_trades_data if t.get('Symbol')]
)))

selected_exchanges = col1.multiselect(
    t('filter_by_exchange'), options=all_exchanges)
selected_symbols = col2.multiselect(t('filter_by_asset'), options=all_symbols)

# --- ФИЛЬТРАЦИЯ ДАННЫХ ---
filtered_open_positions = open_positions_data
filtered_account_balances = account_balances_data

selected_assets_from_symbols = {s.split('/')[0] for s in selected_symbols}

if selected_exchanges:
    filtered_open_positions = [p for p in filtered_open_positions if p.get(
        'Exchange') in selected_exchanges]
    filtered_account_balances = [b for b in filtered_account_balances if b.get(
        'Account_Name') in selected_exchanges]

if selected_symbols:
    filtered_open_positions = [
        p for p in filtered_open_positions if p.get('Symbol') in selected_symbols]
    filtered_account_balances = [
        b for b in filtered_account_balances
        if b.get('Asset') in selected_assets_from_symbols or b.get('Asset') in INVESTMENT_ASSETS
    ]

# --- БЛОК СВОДКИ ПО СТЕЙБЛКОИНАМ ---
st.subheader(t('stablecoin_summary_header'))
stablecoin_balances = [b for b in filtered_account_balances if str(
    b.get('Asset', '')).upper() in INVESTMENT_ASSETS]

if stablecoin_balances:
    df_stables = pd.DataFrame(stablecoin_balances)
    df_stables['Balance_dec'] = df_stables['Balance'].apply(safe_to_decimal)
    total_stables_value = df_stables['Balance_dec'].sum()

    st.metric(f"{t('total_stablecoins')}",
              format_number(total_stables_value, show_currency_symbol=BASE_CURRENCY))

    # ИСПРАВЛЕНО: Добавлен блок детализации по каждому стейблкоину
    st.markdown("##### " + t('details_by_asset'))
    stables_by_asset = df_stables.groupby(
        'Asset')['Balance_dec'].sum().sort_values(ascending=False)
    if not stables_by_asset.empty:
        num_assets = len(stables_by_asset)
        cols_assets = st.columns(num_assets)
        for i, (asset, balance) in enumerate(stables_by_asset.items()):
            if balance > Decimal('0.01'):
                cols_assets[i].metric(asset, format_number(balance))

    # Используем t() для будущего перевода
    st.markdown("##### " + t('Баланс по счетам'))
    stables_by_account = df_stables.groupby(
        'Account_Name')['Balance_dec'].sum().sort_index()
    if not stables_by_account.empty:
        num_accounts = len(stables_by_account)
        cols_accounts = st.columns(min(num_accounts, 5))
        for i, (account, balance) in enumerate(stables_by_account.items()):
            if balance > Decimal('0.01'):
                cols_accounts[i % 5].metric(account, format_number(balance))
else:
    st.info(t('no_stablecoin_data'))
st.divider()

# --- БЛОК ДЕТАЛИЗАЦИИ ПОРТФЕЛЯ ---
st.header(t('portfolio_details_header'))
portfolio_components = []

for pos in filtered_open_positions:
    current_price = safe_to_decimal(pos.get('Current_Price'), None)
    if current_price is not None:
        value_usd = safe_to_decimal(pos.get('Net_Amount')) * current_price
        if value_usd > 0:
            asset_name = str(pos.get('Symbol')).split('/')[0]
            portfolio_components.append({
                'ТипКомпонента': 'Криптоактив', 'Актив_или_Счет': asset_name,
                'Стоимость_USD': value_usd, 'Биржа_Счет': pos.get('Exchange')
            })

for balance in stablecoin_balances:
    balance_dec = safe_to_decimal(balance.get('Balance'))
    if balance_dec > 0:
        portfolio_components.append({
            'ТипКомпонента': 'СвободныеСредства', 'Актив_или_Счет': balance.get('Asset'),
            'Стоимость_USD': balance_dec, 'Биржа_Счет': balance.get('Account_Name')
        })

if not portfolio_components:
    st.info(t('no_portfolio_data'))
else:
    df_portfolio_full = pd.DataFrame(portfolio_components)
    final_total_value = df_portfolio_full['Стоимость_USD'].sum()

    st.metric(t('total_selected_assets_value'), format_number(
        final_total_value, show_currency_symbol=BASE_CURRENCY))

    st.subheader(t('capital_structure_header'))
    df_portfolio_full['Категория'] = df_portfolio_full['ТипКомпонента'].replace({
        'Криптоактив': t('category_crypto'), 'СвободныеСредства': t('category_stables')
    })
    df_capital_structure = df_portfolio_full.groupby(
        'Категория')['Стоимость_USD'].sum().reset_index()

    if not df_capital_structure.empty and df_capital_structure['Стоимость_USD'].sum() > 0:
        df_capital_structure['Formatted_Value'] = df_capital_structure['Стоимость_USD'].apply(
            lambda x: format_number(x, show_currency_symbol=BASE_CURRENCY))
        fig_structure = px.pie(df_capital_structure, values='Стоимость_USD', names='Категория',
                               title=t('capital_distribution_chart_title'), custom_data=['Formatted_Value'])
        fig_structure.update_traces(textposition='inside', textinfo='percent+label',
                                    hovertemplate='<b>%{label}</b><br>'+f'Стоимость: %{{customdata[0]}}<br>Доля: %{{percent}}')
        st.plotly_chart(fig_structure, use_container_width=True)
    else:
        st.info(t('no_capital_structure_data'))

    st.subheader(t('portfolio_details_subheader'))
    df_table = df_portfolio_full.groupby(['Биржа_Счет', 'Актив_или_Счет'])[
        'Стоимость_USD'].sum().reset_index()

    if final_total_value > 0:
        df_table['Доля, %'] = (
            df_table['Стоимость_USD'] / final_total_value) * 100
    else:
        df_table['Доля, %'] = Decimal('0')

    df_table[t('col_value_currency')
             ] = df_table['Стоимость_USD'].apply(format_number)
    df_table[t('col_share_percent')] = df_table['Доля, %'].apply(
        lambda x: f"{format_number(x, '0.01')}%")
    df_table[t('col_asset_source')] = df_table['Актив_или_Счет'] + \
        " (" + df_table['Биржа_Счет'] + ")"

    df_sorted_for_display = df_table.sort_values(
        by='Стоимость_USD', ascending=False)
    cols_to_show_portfolio = [t('col_asset_source'), t(
        'col_value_currency'), t('col_share_percent')]

    st.dataframe(
        df_sorted_for_display[cols_to_show_portfolio],
        use_container_width=True,
        hide_index=True
    )
