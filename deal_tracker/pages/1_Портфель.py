# pages/1_Портфель.py
import streamlit as st
import pandas as pd
from decimal import Decimal
import os
import sys

# --- Настройка путей и импортов ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Импортируем из папки deal_tracker
from deal_tracker.locales import t
from deal_tracker import config, dashboard_utils

# --- НАСТРОЙКА СТРАНИЦЫ ---
st.set_page_config(layout="wide", page_title=t('page_portfolio_title'))
st.title(t('page_portfolio_header'))

CURRENCY_SYMBOLS = {'USD': '$', 'EUR': '€'}
display_currency = CURRENCY_SYMBOLS.get(config.BASE_CURRENCY, config.BASE_CURRENCY)

# --- ЗАГРУЗКА ДАННЫХ ---
all_data, all_errors = dashboard_utils.load_all_data_with_error_handling()
if all_errors:
    with st.expander("⚠️ Обнаружены ошибки при чтении данных из Google Sheets", expanded=True):
        for msg in all_errors: st.error(msg)

positions_data = all_data.get('open_positions', [])
account_balances_data = all_data.get('account_balances', [])
current_prices = dashboard_utils.fetch_current_prices_for_all_exchanges(positions_data)

# --- ФИЛЬТРЫ В БОКОВОЙ ПАНЕЛИ ---
with st.sidebar:
    lang_options = ["ru", "en"]; current_lang = st.session_state.get("lang", "ru"); lang_index = lang_options.index(current_lang) if current_lang in lang_options else 0; lang = st.radio("🌐 Язык / Language", options=lang_options, index=lang_index, key='lang_radio'); st.session_state["lang"] = lang
    st.divider(); st.header(t('filters_header'))
    
    all_exchanges = sorted(list(set([b.account_name for b in account_balances_data] + [p.exchange for p in positions_data])))
    selected_exchanges = st.multiselect(label=t('filter_by_exchange'), options=all_exchanges)

    all_assets = sorted(list(set([p.symbol.split('/')[0] for p in positions_data] + [b.asset for b in account_balances_data])))
    selected_assets = st.multiselect(label=t('filter_by_asset'), options=all_assets)

    if st.button(t('update_button'), key="portfolio_refresh"):
        st.cache_data.clear(); dashboard_utils.invalidate_cache(); st.rerun()

# --- ФИЛЬТРАЦИЯ ДАННЫХ ---
if selected_exchanges:
    positions_data = [p for p in positions_data if p.exchange in selected_exchanges]
    account_balances_data = [b for b in account_balances_data if b.account_name in selected_exchanges]

# --- ОСНОВНАЯ ЛОГИКА ---
portfolio_components = []

# 1. Обработка открытых позиций (криптоактивы)
for pos in positions_data:
    base_asset = pos.symbol.split('/')[0]
    if selected_assets and base_asset not in selected_assets: continue
    price = current_prices.get(pos.exchange.lower(), {}).get(pos.symbol, Decimal('0'))
    if pos.net_amount > 0 and price > 0:
        portfolio_components.append({'category': t('category_crypto'), 'asset': base_asset, 'value_usd': pos.net_amount * price, 'location': pos.exchange, 'current_price': price})

# 2. Обработка свободных средств (стейблкоины)
for balance in account_balances_data:
    if balance.asset in config.INVESTMENT_ASSETS and balance.balance > 0:
        if selected_assets and balance.asset not in selected_assets: continue
        portfolio_components.append({'category': t('category_stables'), 'asset': balance.asset, 'value_usd': balance.balance, 'location': balance.account_name, 'current_price': Decimal('1.0')})


# --- ОТОБРАЖЕНИЕ ---
if not portfolio_components:
    st.info(t('no_portfolio_data_after_filter'))
else:
    df_portfolio = pd.DataFrame(portfolio_components)
    total_portfolio_value = df_portfolio['value_usd'].sum()
    st.metric(t('total_selected_assets_value'), dashboard_utils.format_number(total_portfolio_value, currency_symbol=display_currency))
    st.divider()

    # --- ВИЗУАЛИЗАЦИЯ С ДВУМЯ ГРАФИКАМИ ---
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"#### {t('asset_allocation_header')}")
        asset_dist = df_portfolio.groupby('asset')['value_usd'].sum().reset_index()
        fig_assets = dashboard_utils.create_pie_chart(asset_dist, t('asset_distribution_title'), 'asset', 'value_usd')
        st.plotly_chart(fig_assets, use_container_width=True)

    with col2:
        st.markdown(f"#### {t('location_allocation_header')}")
        location_dist = df_portfolio.groupby('location')['value_usd'].sum().reset_index()
        fig_locations = dashboard_utils.create_pie_chart(location_dist, t('location_distribution_title'), 'location', 'value_usd')
        st.plotly_chart(fig_locations, use_container_width=True)

    st.divider()

    # --- Таблица с детализацией (простая версия) ---
    st.markdown(f"#### {t('asset_details_header')}")
    df_details = df_portfolio.groupby(['location', 'asset'])['value_usd'].sum().reset_index()
    
    price_map = df_portfolio.groupby('asset')['current_price'].first()
    df_details['current_price'] = df_details['asset'].map(price_map)
    df_details['share'] = (df_details['value_usd'] / total_portfolio_value * 100) if total_portfolio_value > 0 else Decimal('0')
    
    df_display = df_details.sort_values(by='value_usd', ascending=False)
    
    # Форматирование и переименование колонок
    df_display[t('col_asset')] = df_display['asset']
    df_display[t('col_location')] = df_display['location']
    df_display[t('col_current_price')] = df_display['current_price'].apply(lambda x: dashboard_utils.format_number(x, precision_str='0.0001', currency_symbol=display_currency))
    df_display[t('col_value')] = df_display['value_usd'].apply(lambda x: dashboard_utils.format_number(x, currency_symbol=display_currency))
    df_display[t('col_share_percent')] = df_display['share'].apply(lambda x: f"{dashboard_utils.format_number(x)}%")
    
    st.dataframe(
        df_display[[t('col_asset'), t('col_location'), t('col_current_price'), t('col_value'), t('col_share_percent')]],
        hide_index=True,
        use_container_width=True
    )