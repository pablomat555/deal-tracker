# pages/2_Движения_Средств.py
import streamlit as st
import pandas as pd
from decimal import Decimal
import os
import sys

# --- Настройка путей и импортов ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from deal_tracker.locales import t
from deal_tracker import config, dashboard_utils

# --- НАСТРОЙКА СТРАНИЦЫ ---
st.set_page_config(layout="wide", page_title=t('page_movements_title'))
st.title(t('page_movements_header'))

CURRENCY_SYMBOLS = {'USD': '$', 'EUR': '€'}
display_currency = CURRENCY_SYMBOLS.get(config.BASE_CURRENCY, config.BASE_CURRENCY)

# --- [НОВОЕ] Вспомогательная функция для красивых виджетов ---
def render_movement_metric(label: str, value: Decimal, color: str):
    """Отображает метрику с цветовой подсветкой."""
    formatted_value = dashboard_utils.format_number(value, currency_symbol=display_currency)
    html = f"""
    <div style="padding: 5px; border: 1px solid #3a3a3a; border-radius: 8px; text-align: center; height: 100%;">
        <div style="font-size: 0.9em; color: #9ca3af;">{label}</div>
        <div style="color: {color}; font-size: 1.4em; font-weight: 600;">{formatted_value}</div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)

# --- ЗАГРУЗКА ДАННЫХ ---
all_data, all_errors = dashboard_utils.load_all_data_with_error_handling()
if all_errors:
    with st.expander("⚠️ Обнаружены ошибки при чтении данных из Google Sheets", expanded=True):
        for msg in all_errors: st.error(f"- {msg}")

# Извлекаем нужные данные
fund_movements_data = all_data.get('fund_movements', [])
account_balances_data = all_data.get('account_balances', []) # Для фильтров

# --- ОСНОВНАЯ ЛОГИКА ---
if not fund_movements_data:
    st.info(t('no_movements_data'))
else:
    # Создаем DataFrame один раз
    df = pd.DataFrame([m.__dict__ for m in fund_movements_data])
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['amount'] = df['amount'].apply(Decimal)

    # --- [НОВЫЙ БЛОК] Расчет и отображение итоговых сумм ---
    total_deposited = df[df['movement_type'] == 'DEPOSIT']['amount'].sum()
    total_withdrawn = df[df['movement_type'] == 'WITHDRAWAL']['amount'].sum()
    
    col1, col2, col3 = st.columns([1.5, 1.5, 4])
    with col1:
        render_movement_metric(t("Всего введено"), total_deposited, "#16A34A") # Зеленый
    with col2:
        render_movement_metric(t("Всего выведено"), total_withdrawn, "#DC2626") # Красный
    
    st.divider()

    # --- ФИЛЬТРЫ В БОКОВОЙ ПАНЕЛИ ---
    with st.sidebar:
        lang_options = ["ru", "en"]; current_lang = st.session_state.get("lang", "ru"); lang_index = lang_options.index(current_lang) if current_lang in lang_options else 0; lang = st.radio("🌐 Язык / Language", options=lang_options, index=lang_index, key='lang_radio_movements'); st.session_state["lang"] = lang
        st.divider(); st.header(t('filters_header'))
        
        source_accounts = df['source_name'].dropna().unique()
        dest_accounts = df['destination_name'].dropna().unique()
        balance_accounts = [b.account_name for b in account_balances_data if b.account_name]
        all_accounts = sorted(list(set(list(source_accounts) + list(dest_accounts) + balance_accounts)))
        
        selected_accounts = st.multiselect(label=t('filter_by_exchange'), options=all_accounts)

        all_assets = sorted(df['asset'].dropna().unique())
        selected_assets = st.multiselect(label=t('filter_by_asset'), options=all_assets)

        if st.button(t('update_button'), key="movements_refresh"):
            st.cache_data.clear(); dashboard_utils.invalidate_cache(); st.rerun()

    # --- ФИЛЬТРАЦИЯ DATAFRAME ---
    filtered_df = df.copy()
    if selected_accounts:
        filtered_df = filtered_df[
            filtered_df['source_name'].isin(selected_accounts) |
            filtered_df['destination_name'].isin(selected_accounts)
        ]
    if selected_assets:
        filtered_df = filtered_df[filtered_df['asset'].isin(selected_assets)]
    
    # --- ОТОБРАЖЕНИЕ ТАБЛИЦЫ ---
    if filtered_df.empty:
        st.info(t('no_data_for_display'))
    else:
        st.markdown(f"#### {t('Детализация движений')}")
        df_display = pd.DataFrame()
        TYPE_ICONS = {'DEPOSIT': '🟢', 'WITHDRAWAL': '🔴', 'TRANSFER': '🔵'}
        
        df_display[t('col_type')] = filtered_df['movement_type'].apply(lambda x: f"{TYPE_ICONS.get(x, '⚪️')} {x.capitalize()}" if x else '⚪️')
        df_display[t('col_date')] = filtered_df['timestamp']
        df_display[t('col_amount')] = filtered_df.apply(lambda row: dashboard_utils.format_number(row['amount'], precision_str=dashboard_utils.get_precision_for_asset(row['asset'])), axis=1)
        df_display[t('col_currency')] = filtered_df['asset']
        df_display[t('col_from')] = filtered_df['source_name'].fillna(t('external_source'))
        df_display[t('col_to')] = filtered_df['destination_name'].fillna(t('external_destination'))
        df_display[t('col_notes')] = filtered_df['notes'].fillna('')

        st.dataframe(
            df_display.sort_values(by=t('col_date'), ascending=False),
            use_container_width=True, 
            hide_index=True,
            column_config={
                t('col_date'): st.column_config.DatetimeColumn(label=t('col_date'), format="YYYY-MM-DD HH:mm"),
            }
        )
