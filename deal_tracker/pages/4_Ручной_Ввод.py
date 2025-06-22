# pages/4_Ручной_Ввод.py
from locales import t
import utils
from trade_logger import log_trade, log_fund_movement
import config
import streamlit as st
import logging
import time
from decimal import Decimal
import datetime
import os
import sys

# Добавляем корень проекта в путь
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.append(project_root)


# --- НАСТРОЙКИ И ФОРМЫ ---
st.set_page_config(layout="wide", page_title="Ручной Ввод")
st.title("📝 Ручной Ввод Данных")


def display_manual_trade_entry_form():
    with st.form(key="manual_trade_form", clear_on_submit=True):
        # ... (код вашей формы остается без изменений) ...
        # ... (col1, col2, col3, st.text_input и т.д.) ...
        submitted = st.form_submit_button("Добавить сделку")
        if submitted:
            amount_dec = Decimal(str(amount))
            price_dec = Decimal(str(price))

            if not symbol or amount_dec <= 0 or price_dec <= 0:
                st.error("❌ Поля 'Символ', 'Количество' и 'Цена' обязательны.")
                return

            # ИСПРАВЛЕНО: Правильный вызов log_trade с типизированными данными
            timestamp = utils.parse_datetime_from_args(
                {'date': trade_date_str})
            kwargs = {'notes': notes}  # Добавьте сюда другие опц. поля

            success, msg = log_trade(
                trade_type=trade_type, exchange=exchange, symbol=symbol,
                amount=amount_dec, price=price_dec, timestamp=timestamp, **kwargs
            )
            if success:
                st.success(f"✅ Сделка добавлена! ID: {msg}")
            else:
                st.error(f"❌ Ошибка: {msg}")

# ... (код для форм движения средств переписывается по аналогии) ...


# --- ГЛАВНАЯ ЧАСТЬ СТРАНИЦЫ ---
tab_trade, tab_movement = st.tabs(["📈 Сделки", "💸 Движения Средств"])
with tab_trade:
    display_manual_trade_entry_form()

# ...
