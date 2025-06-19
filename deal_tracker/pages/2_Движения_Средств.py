# pages/2_Движения_Средств.py
import streamlit as st
import pandas as pd
import logging

# ИСПРАВЛЕНО: Правильные импорты
import dashboard_utils
from locales import t

# --- НАСТРОЙКА СТРАНИЦЫ И ЗАГРУЗКА ---
st.set_page_config(layout="wide", page_title=t('page_movements_title'))
st.title(t('page_movements_header'))
if st.button(t('refresh_page_button'), key="movements_refresh"):
    st.cache_data.clear()
    st.rerun()

all_data = dashboard_utils.load_all_dashboard_data()
movements = all_data.get('fund_movements', [])

# --- ОСНОВНАЯ ЛОГИКА ---
if not movements:
    st.info(t('no_movements_data'))
else:
    df = pd.DataFrame(movements)

    # Создаем форматированные колонки для отображения
    df_display = pd.DataFrame()
    df_display[t('col_date')] = df['timestamp']
    df_display[t('col_type')] = df['movement_type']
    df_display[t('col_amount')] = df['amount'].map(
        lambda x: dashboard_utils.format_number(x, '0.01'))
    df_display[t('col_currency')] = df['asset']
    df_display[t('col_from')] = df['source_name'].fillna('Внешний')
    df_display[t('col_to')] = df['destination_name'].fillna('Внешний')
    df_display[t('col_notes')] = df['notes'].fillna('')

    st.dataframe(
        df_display.sort_values(by=t('col_date'), ascending=False),
        use_container_width=True,
        hide_index=True,
        column_config={
            t('col_date'): st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm:ss")
        }
    )
