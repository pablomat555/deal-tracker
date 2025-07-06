# deal_tracker/pages/1_Портфель.py
import streamlit as st
import pandas as pd
from decimal import Decimal
import plotly.express as px
import os
import sys

# --- Настройка путей и импортов ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

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
        for msg in all_errors:
            st.error(f"- {msg}")

positions_data = all_data.get('open_positions', [])
account_balances_data = all_data.get('account_balances', [])
current_prices = dashboard_utils.fetch_current_prices_for_all_exchanges(positions_data)

# --- ФИЛЬТРЫ В БОКОВОЙ ПАНЕЛИ ---
with st.sidebar:
    lang_options = ["ru", "en"]
    current_lang = st.session_state.get("lang", "ru")
    lang_index = lang_options.index(current_lang) if current_lang in lang_options else 0
    lang = st.radio("🌐 Язык / Language", options=lang_options, index=lang_index, key='lang_radio_portfolio')
    st.session_state["lang"] = lang
    st.divider()
    st.header(t('filters_header'))
    
    all_exchanges = sorted(list(set([b.account_name.capitalize() for b in account_balances_data if b.account_name] + [p.exchange.capitalize() for p in positions_data if p.exchange])))
    selected_exchanges = st.multiselect(label=t('filter_by_exchange'), options=all_exchanges)
    
    all_assets = sorted(list(set([p.symbol.split('/')[0] for p in positions_data if p.symbol] + [b.asset for b in account_balances_data if b.asset])))
    selected_assets = st.multiselect(label=t('filter_by_asset'), options=all_assets)
    
    if st.button(t('update_button'), key="portfolio_refresh"):
        st.cache_data.clear()
        dashboard_utils.invalidate_cache()
        st.rerun()

# --- ОСНОВНАЯ ЛОГИКА ---
portfolio_components = []

# 1. Обработка открытых позиций (криптовалют)
for pos in positions_data:
    if not pos.symbol or not pos.net_amount or not pos.exchange or not pos.avg_entry_price:
        continue
    base_asset = pos.symbol.split('/')[0]
    location = pos.exchange.capitalize()
    
    price = current_prices.get(pos.exchange.lower(), {}).get(pos.symbol, Decimal('0'))
    if pos.net_amount > 0:
        portfolio_components.append({
            'asset': base_asset,
            'quantity': pos.net_amount,
            'value_usd': pos.net_amount * price,
            'location': location,
            'current_price': price,
            'avg_entry_price': pos.avg_entry_price,
            'purchase_value': pos.net_amount * pos.avg_entry_price # Стоимость покупки
        })

# 2. Обработка стейблкоинов и других балансов
for balance in account_balances_data:
    if not balance.account_name or not balance.asset or not balance.balance:
        continue
    location = balance.account_name.capitalize()
    
    if balance.asset in config.INVESTMENT_ASSETS and balance.balance > 0:
        portfolio_components.append({
            'asset': balance.asset,
            'quantity': balance.balance,
            'value_usd': balance.balance,
            'location': location,
            'current_price': Decimal('1.0'),
            'avg_entry_price': Decimal('1.0'),
            'purchase_value': balance.balance # Для стейблов стоимость покупки равна их количеству
        })

# --- ФИЛЬТРАЦИЯ ДАННЫХ ПОРТФЕЛЯ ---
if portfolio_components:
    df_portfolio = pd.DataFrame(portfolio_components)
    if selected_exchanges:
        df_portfolio = df_portfolio[df_portfolio['location'].isin(selected_exchanges)]
    if selected_assets:
        df_portfolio = df_portfolio[df_portfolio['asset'].isin(selected_assets)]
else:
    df_portfolio = pd.DataFrame()

# --- ОТОБРАЖЕНИЕ ---
if df_portfolio.empty:
    st.info(t('no_portfolio_data_after_filter'))
else:
    total_portfolio_value = df_portfolio['value_usd'].sum()
    st.metric(t('total_selected_assets_value'), dashboard_utils.format_number(total_portfolio_value, currency_symbol=display_currency))
    st.divider()

    # Визуализация
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

    # Таблица с детализацией
    st.markdown(f"#### {t('asset_details_header')}")
    
    # 1. Группируем данные и считаем средневзвешенную цену
    df_portfolio['entry_value'] = df_portfolio['avg_entry_price'] * df_portfolio['quantity']
    df_details = df_portfolio.groupby(['location', 'asset']).agg(
        quantity=('quantity', 'sum'),
        value_usd=('value_usd', 'sum'),
        purchase_value=('purchase_value', 'sum'),
        total_entry_value=('entry_value', 'sum')
    ).reset_index()
    
    df_details['avg_entry_price'] = df_details.apply(
        lambda row: row['total_entry_value'] / row['quantity'] if row['quantity'] > 0 else 0, axis=1)
    
    # 2. Добавляем текущие цены и рассчитываем новые показатели
    price_map = df_portfolio.groupby('asset')['current_price'].first()
    df_details['current_price'] = df_details['asset'].map(price_map).fillna(0)
    
    df_details['price_change_pct'] = df_details.apply(
        lambda row: ((row['current_price'] / row['avg_entry_price']) - 1) * 100 if row['avg_entry_price'] > 0 else 0, axis=1)
    
    df_details['share'] = (df_details['value_usd'] / total_portfolio_value * 100) if total_portfolio_value > 0 else Decimal('0')
    
    df_sorted = df_details.sort_values(by='value_usd', ascending=False)
    
    # 3. Форматируем DataFrame для вывода
    df_display = pd.DataFrame()
    df_display[t('col_asset')] = df_sorted['asset']
    df_display[t('col_location')] = df_sorted['location']
    df_display[t('col_qty')] = df_sorted.apply(lambda r: dashboard_utils.format_number(r['quantity'], dashboard_utils.get_precision_for_asset(r['asset'])), axis=1)
    df_display[t('col_avg_entry')] = df_sorted.apply(lambda r: dashboard_utils.format_number(r['avg_entry_price'], dashboard_utils.get_price_precision(r['asset'])), axis=1)
    # [НОВАЯ КОЛОНКА]
    df_display[t('col_purchase_value')] = df_sorted['purchase_value'].apply(lambda x: dashboard_utils.format_number(x, currency_symbol=display_currency))
    df_display[t('col_current_price')] = df_sorted.apply(lambda r: dashboard_utils.format_number(r['current_price'], dashboard_utils.get_price_precision(r['asset'])), axis=1)
    # [НОВАЯ КОЛОНКА]
    df_display[t('col_price_change_pct')] = df_sorted['price_change_pct'].apply(lambda x: dashboard_utils.format_number(x, add_plus_sign=True, precision_str="0.01") + "%")
    df_display[t('col_value')] = df_sorted['value_usd'].apply(lambda x: dashboard_utils.format_number(x, currency_symbol=display_currency))
    df_display[t('col_share_percent')] = df_sorted['share'].apply(lambda x: f"{dashboard_utils.format_number(x)}%")
    
    # 4. Задаем порядок колонок и применяем стили
    column_order = [
        t('col_asset'), t('col_location'), t('col_qty'), 
        t('col_avg_entry'), t('col_purchase_value'), t('col_current_price'), 
        t('col_price_change_pct'), t('col_value'), t('col_share_percent')
    ]
    
    styler = df_display[column_order].style.applymap(
        dashboard_utils.style_pnl_value, 
        subset=[t('col_price_change_pct')]
    )
    
    # [ВОССТАНОВЛЕНО] Используем стандартный st.dataframe
    st.dataframe(styler, hide_index=True, use_container_width=True)