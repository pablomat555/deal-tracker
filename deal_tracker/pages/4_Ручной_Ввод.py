# pages/4_Ручной_Ввод.py
from locales import t
from trade_logger import log_trade, log_fund_movement
import config
import utils
from datetime import datetime, time as dt_time
from decimal import Decimal
import time
import logging
import streamlit as st
import os
import sys

# Добавляем корень проекта в путь.
# Для файла в pages/.. -> это папка deal_tracker.
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# ИСПРАВЛЕНО: Используем простые, прямые импорты


# --- НАСТРОЙКИ И ФОРМЫ ---
st.set_page_config(layout="wide", page_title="Ручной Ввод")
st.title("📝 Ручной Ввод Данных")


def display_manual_trade_entry_form():
    """Отображает форму для ручного ввода сделки."""
    st.subheader("📈 Добавить сделку")
    with st.form(key="manual_trade_form", clear_on_submit=True):

        # --- БЛОК ДЛЯ ВВОДА ДАННЫХ ---
        col1, col2 = st.columns(2)
        with col1:
            trade_type = st.radio(
                "Тип сделки", ["BUY", "SELL"], horizontal=True)
            symbol = st.text_input("Символ (например, BTC/USDT)").upper()
            exchange = st.selectbox("Биржа", config.KNOWN_EXCHANGES)
        with col2:
            amount = st.number_input(
                "Количество", min_value=0.0, step=0.0001, format="%.8f")
            price = st.number_input(
                "Цена", min_value=0.0, step=0.0001, format="%.8f")
            # Для ввода даты и времени используем два виджета
            trade_date = st.date_input("Дата сделки", value=datetime.now())
            trade_time = st.time_input(
                "Время сделки", value=datetime.now().time())

        notes = st.text_area("Заметки (опционально)")
        # --- КОНЕЦ БЛОКА ВВОДА ---

        submitted = st.form_submit_button("Добавить сделку")
        if submitted:
            # Проверяем, что числовые поля не пустые перед преобразованием
            if amount <= 0 or price <= 0:
                st.error("❌ 'Количество' и 'Цена' должны быть больше нуля.")
                return

            amount_dec = Decimal(str(amount))
            price_dec = Decimal(str(price))

            if not symbol:
                st.error("❌ Поле 'Символ' обязательно для заполнения.")
                return

            # Собираем дату и время в один объект datetime
            timestamp = datetime.combine(trade_date, trade_time)

            kwargs = {'notes': notes}

            success, msg = log_trade(
                trade_type=trade_type, exchange=exchange, symbol=symbol,
                amount=amount_dec, price=price_dec, timestamp=timestamp, **kwargs
            )
            if success:
                st.success(f"✅ Сделка добавлена! ID: {msg}")
                st.balloons()
            else:
                st.error(f"❌ Ошибка: {msg}")


def display_manual_movement_form():
    """Отображает форму для ручного ввода движения средств."""
    st.subheader("💸 Добавить движение средств")
    # TODO: Реализовать форму для вводов/выводов/переводов по аналогии с формой для сделок
    st.info("Форма для ввода движений средств находится в разработке.")


# --- ГЛАВНАЯ ЧАСТЬ СТРАНИЦЫ ---
tab_trade, tab_movement = st.tabs(["📈 Сделки", "💸 Движения Средств"])
with tab_trade:
    display_manual_trade_entry_form()

with tab_movement:
    display_manual_movement_form()
