# pages/2_Движения_Средств.py

# --- НАЧАЛО УНИВЕРСАЛЬНОГО БЛОКА ---
from deal_tracker.locales import t
from deal_tracker import dashboard_utils
import logging
import pandas as pd
import streamlit as st
import sys
import os

# 1. Добавляем корневую папку проекта в системный путь
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 2. Теперь импорты делаем явными, от имени главного пакета
# Явные импорты из пакета deal_tracker
# --- КОНЕЦ УНИВЕРСАЛЬНОГО БЛОКА ---


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
    # DataFrame создается напрямую из списка моделей
    df = pd.DataFrame([m.__dict__ for m in movements])

    df_display = pd.DataFrame()
    df_display[t('col_date')] = pd.to_datetime(df['timestamp'])
    df_display[t('col_type')] = df['movement_type']
    df_display[t('col_amount')] = df['amount'].map(
        lambda x: dashboard_utils.format_number(x, precision_key='QTY_DISPLAY_PRECISION'))
    df_display[t('col_currency')] = df['asset']
    df_display[t('col_from')] = df['source_name'].fillna(t('external_source'))
    df_display[t('col_to')] = df['destination_name'].fillna(
        t('external_destination'))
    df_display[t('col_notes')] = df['notes'].fillna('')

    st.dataframe(
        df_display.sort_values(by=t('col_date'), ascending=False),
        use_container_width=True, hide_index=True,
        column_config={
            t('col_date'): st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm"),
            t('col_amount'): st.column_config.TextColumn()
        }
    )
