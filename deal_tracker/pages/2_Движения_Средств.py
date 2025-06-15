# pages/2_Движения_Средств.py
import streamlit as st
import pandas as pd
import os
import sys

try:
    from utils import load_all_dashboard_data, format_number
    from locales import t
    import config
except ImportError:
    project_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.append(project_root)
    from utils import load_all_dashboard_data, format_number
    from locales import t
    import config

# --- НАСТРОЙКА СТРАНИЦЫ, ШРИФТА И ЯЗЫКА ---
st.set_page_config(layout="wide", page_title=t('page_movements_title'))
st.markdown(
    """<style>@import url('https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700&display=swap'); html, body, [class*="st-"], [class*="css-"] {font-family: 'Roboto', sans-serif;}</style>""", unsafe_allow_html=True)
st.sidebar.radio("Язык/Language", options=['ru', 'en'],
                 format_func=lambda x: "Русский" if x == 'ru' else "English", key='lang')

# --- ЗАГОЛОВОК И КНОПКА ОБНОВЛЕНИЯ ---
st.title(t('page_movements_header'))
if st.button(t('refresh_page_button'), key="movements_page_refresh"):
    st.cache_data.clear()
    st.rerun()


def display_fund_movements_table(movements_data: list):
    """
    Отображает таблицу движений средств с правильными заголовками и форматированием.
    """
    if not movements_data:
        st.info(t('no_movements_data'))
        return

    df = pd.DataFrame(movements_data)

    # Конвертируем Timestamp в дату для сортировки
    df[t('col_date')] = pd.to_datetime(df['Timestamp'], errors='coerce')
    df = df.sort_values(by=t('col_date'), ascending=False)

    # Применяем форматирование к числовым колонкам
    df[t('col_amount')] = df['Amount'].apply(
        lambda x: format_number(x, '0.01'))

    df_display = df.rename(columns={
        'Type': t('col_type'),
        'Asset': t('col_currency'),
        'Source_Name': t('col_from'),
        'Destination_Name': t('col_to'),
        'Notes': t('col_notes')
    })

    # Список колонок для финального отображения
    display_columns = [
        t('col_date'), t('col_type'), t('col_amount'), t('col_currency'),
        t('col_from'), t('col_to'), t('col_notes')
    ]
    final_cols = [col for col in display_columns if col in df_display.columns]

    st.dataframe(
        df_display[final_cols],
        use_container_width=True,
        hide_index=True,
        column_config={
            t('col_date'): st.column_config.DatetimeColumn(
                format="YYYY-MM-DD HH:mm:ss",
            )
        }
    )


# --- ОСНОВНАЯ ЧАСТЬ СТРАНИЦЫ ---
all_data = load_all_dashboard_data()
fund_movements_list = all_data.get('fund_movements', [])
account_balances_data = all_data.get('account_balances', [])

# --- НОВЫЙ БЛОК: ФИЛЬТРЫ ---
st.markdown("---")
col1, col2 = st.columns(2)

# Собираем уникальные значения для фильтров
all_accounts = sorted(list(set(
    [b.get('Account_Name')
     for b in account_balances_data if b.get('Account_Name')]
)))
all_assets = sorted(list(set(
    [m.get('Asset') for m in fund_movements_list if m.get('Asset')]
)))

selected_accounts = col1.multiselect(
    t('filter_by_exchange'), options=all_accounts)
# Здесь 'asset' используется как валюта движения
selected_assets = col2.multiselect(t('filter_by_asset'), options=all_assets)

# --- НОВЫЙ БЛОК: ФИЛЬТРАЦИЯ ДАННЫХ ---
filtered_movements = fund_movements_list

if selected_accounts:
    filtered_movements = [
        m for m in filtered_movements
        if m.get('Source_Name') in selected_accounts or m.get('Destination_Name') in selected_accounts
    ]

if selected_assets:
    filtered_movements = [
        m for m in filtered_movements if m.get('Asset') in selected_assets]


# --- ИЗМЕНЕНО: Передаем отфильтрованные данные в функцию отображения ---
display_fund_movements_table(filtered_movements)
