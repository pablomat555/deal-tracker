# pages/4_Ручной_Ввод.py
import streamlit as st
import logging
import time
from decimal import Decimal
from datetime import datetime

import os
import sys

# --- Настройка путей и импортов ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Теперь импорты должны работать надежно
from deal_tracker.locales import t
from deal_tracker import config
# Прямые импорты из trade_logger не нужны, если есть utils
from deal_tracker import utils
from deal_tracker import sheets_service # Импортируем напрямую для записи

# --- НАСТРОЙКА СТРАНИЦЫ И ЛОГГЕР ---
st.set_page_config(layout="wide", page_title="Ручной Ввод")
logger = logging.getLogger(__name__)

# --- 1. ОПРЕДЕЛЕНИЕ ВСЕХ ФУНКЦИЙ ---

def display_manual_trade_form():
    """Отображает форму для ручного ввода сделки."""
    st.subheader("📈 " + t("Добавить сделку"))
    with st.form(key="manual_trade_form", clear_on_submit=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            trade_type = st.radio(t("Тип сделки"), ["BUY", "SELL"], horizontal=True)
            symbol = st.text_input(t("Символ (напр., BTC/USDT)"), "").upper()
            exchange = st.selectbox(t("Биржа"), config.KNOWN_EXCHANGES)
        with col2:
            amount_str = st.text_input(t("Количество"))
            price_str = st.text_input(t("Цена"))
        with col3:
            date_str = st.text_input(t("Дата и время (ГГГГ-ММ-ДД ЧЧ:ММ)"), placeholder=t("Пусто = текущее время"))

        notes = st.text_area(t("Заметки (опционально)"))
        
        submitted = st.form_submit_button(t("Добавить сделку"))
        if submitted:
            amount_dec = utils.parse_decimal(amount_str)
            price_dec = utils.parse_decimal(price_str)

            if not all([symbol, amount_dec, price_dec]) or amount_dec <= 0 or price_dec <= 0:
                st.error(t("Поля 'Символ', 'Количество' и 'Цена' обязательны и должны быть > 0."))
                return

            timestamp = utils.parse_datetime_from_args({'date': date_str} if date_str else {})

            with st.spinner(t("Обработка...")):
                # Используем utils для создания объекта и sheets_service для записи
                trade_data_obj = utils.create_trade_data_from_raw(
                    trade_type=trade_type, exchange=exchange, symbol=symbol,
                    amount=amount_dec, price=price_dec, timestamp=timestamp, notes=notes
                )
                success = sheets_service.add_trade(trade_data_obj)
                msg = trade_data_obj.trade_id if success else "Ошибка записи в Google Sheets"
            
            if success:
                st.success(t("✅ Сделка добавлена! ID:") + f" {msg}"); st.balloons(); time.sleep(2); st.rerun()
            else:
                st.error(f"❌ {t('Ошибка')}: {msg}")


def display_manual_movement_forms():
    """Отображает формы для ввода/вывода/перевода средств."""
    st.subheader("💸 " + t("Добавить движение средств"))
    
    movement_type = st.selectbox(t("Тип операции"), ["DEPOSIT", "WITHDRAWAL", "TRANSFER"])

    def handle_submission(m_type, asset, amount_str, source, dest, date_str, notes):
        amount = utils.parse_decimal(amount_str)
        if not all([asset, amount]) or amount <= 0:
            st.error(t("Поля 'Актив' и 'Сумма' обязательны и должны быть > 0."))
            return

        timestamp = utils.parse_datetime_from_args({'date': date_str} if date_str else {})

        with st.spinner(t("Обработка...")):
            # Используем utils для создания объекта и sheets_service для записи
            movement_data_obj = utils.create_movement_data_from_raw(
                movement_type=m_type, asset=asset, amount=amount, timestamp=timestamp,
                source_name=source, destination_name=dest, notes=notes
            )
            success = sheets_service.add_movement(movement_data_obj)
            msg = movement_data_obj.movement_id if success else "Ошибка записи в Google Sheets"
        
        if success:
            st.success(t("✅ Операция '{m_type}' успешно добавлена! ID:").format(m_type=m_type) + f" {msg}")
            st.balloons()
            time.sleep(2)
            st.rerun()
        else:
            st.error(f"❌ {t('Ошибка')}: {msg}")

    with st.form(key=f"{movement_type}_form", clear_on_submit=True):
        col1, col2, col3 = st.columns(3)
        asset = col1.text_input(t("Актив (напр., USDT)"), key=f"asset_{movement_type}").upper()
        amount_str = col2.text_input(t("Сумма"), key=f"amount_{movement_type}")
        date_str = col3.text_input(t("Дата и время (ГГГГ-ММ-ДД ЧЧ:ММ)"), placeholder=t("Пусто = текущее время"), key=f"date_{movement_type}")

        source, dest = None, None
        if movement_type == "DEPOSIT":
            dest = st.selectbox(t("Счет назначения (КУДА)"), config.KNOWN_EXCHANGES + config.KNOWN_WALLETS, key="dest_dep")
        elif movement_type == "WITHDRAWAL":
            source = st.selectbox(t("Счет списания (ОТКУДА)"), config.KNOWN_EXCHANGES + config.KNOWN_WALLETS, key="source_with")
        elif movement_type == "TRANSFER":
            c1, c2 = st.columns(2)
            source = c1.selectbox(t("Счет списания (ОТКУДА)"), config.KNOWN_EXCHANGES + config.KNOWN_WALLETS, key="source_trans")
            dest = c2.selectbox(t("Счет назначения (КУДА)"), config.KNOWN_EXCHANGES + config.KNOWN_WALLETS, key="dest_trans")

        notes = st.text_area(t("Заметки (опционально)"), key=f"notes_{movement_type}")
        submitted = st.form_submit_button(t("Добавить") + f" {movement_type.lower()}")
        if submitted:
            handle_submission(movement_type, asset, amount_str, source, dest, date_str, notes)

# --- 2. ГЛАВНЫЙ КОД (ВЫЗЫВАЕТ ФУНКЦИИ ПОСЛЕ ИХ ОПРЕДЕЛЕНИЯ) ---
st.title("📝 " + t("Ручной Ввод Данных"))
st.caption(t("Эта страница предназначена для ручного добавления сделок и финансовых операций в систему."))

tab_trade, tab_movement = st.tabs([t("📈 Сделки"), t("💸 Движения Средств")])

with tab_trade:
    # [ИСПРАВЛЕНО] Опечатка в названии функции
    display_manual_trade_form()

with tab_movement:
    display_manual_movement_forms()
