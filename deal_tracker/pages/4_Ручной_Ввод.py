# pages/4_Ручной_Ввод.py
import streamlit as st
import logging
import time
from decimal import Decimal
import datetime
import os
import sys

# --- ИСПРАВЛЕНО: Правильная обработка импортов ---
try:
    import config
    from trade_logger import log_trade, log_fund_movement
    import utils
    from locales import t
except ImportError:
    project_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.append(project_root)
    import config
    from trade_logger import log_trade, log_fund_movement
    import utils
    from locales import t

# Настройка логгера
_logger = logging.getLogger(__name__)

# --- НАСТРОЙКА СТРАНИЦЫ (единоразово вверху) ---
st.set_page_config(layout="wide", page_title="Ручной Ввод")
st.markdown(
    """<style>@import url('https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700&display=swap'); html, body, [class*="st-"], [class*="css-"] {font-family: 'Roboto', sans-serif;}</style>""", unsafe_allow_html=True)
st.sidebar.radio("Язык/Language", options=['ru', 'en'],
                 format_func=lambda x: "Русский" if x == 'ru' else "English", key='lang')


# --- ХЕЛПЕРЫ ---
def _determine_entity_type(name: str, default_type_if_unknown="EXTERNAL") -> str:
    """Определяет тип сущности (биржа, кошелек, внешняя) по имени."""
    if not name:
        return default_type_if_unknown
    name_lower = name.strip().lower()
    known_exchanges = getattr(config, 'KNOWN_EXCHANGES', [])
    known_wallets = getattr(config, 'KNOWN_WALLETS', [])
    if name_lower in [exch.strip().lower() for exch in known_exchanges if isinstance(exch, str)]:
        return "EXCHANGE"
    if name_lower in [w.strip().lower() for w in known_wallets if isinstance(w, str)]:
        return "WALLET"
    return default_type_if_unknown

# --- ФОРМЫ ВВОДА ---


def display_manual_trade_entry_form():
    """Отображает форму для ручного ввода и логирования сделки."""
    with st.form(key="manual_trade_form", clear_on_submit=True):
        st.subheader("Основные параметры сделки")
        col1, col2, col3 = st.columns(3)
        with col1:
            trade_type = st.selectbox(
                "Тип сделки", ["BUY", "SELL"], key="trade_type_select")
            # ИСПРАВЛЕНО: Список бирж берется из config.py
            known_exchanges = getattr(config, 'KNOWN_EXCHANGES', [])
            exchange = st.selectbox(
                "Биржа", options=known_exchanges, key="exchange_select")
        with col2:
            symbol = st.text_input("Символ (например, ETH/USDT)")
            amount = st.number_input(
                "Количество", min_value=0.0, step=0.0001, format="%f")
        with col3:
            price = st.number_input(
                "Цена", min_value=0.0, step=0.01, format="%f")
            trade_date_str = st.text_input(
                "Дата (ГГГГ-ММ-ДД ЧЧ:ММ:СС)", placeholder="Пусто = текущее время")

        st.divider()
        st.subheader("Дополнительные параметры (опционально)")
        col_add1, col_add2, col_add3 = st.columns(3)
        with col_add1:
            sl_price = st.number_input(
                "Stop Loss (SL)", value=0.0, min_value=0.0, format="%f")
            tp1_price = st.number_input(
                "Take Profit 1 (TP1)", value=0.0, min_value=0.0, format="%f")
        with col_add2:
            tp2_price = st.number_input(
                "Take Profit 2 (TP2)", value=0.0, min_value=0.0, format="%f")
            tp3_price = st.number_input(
                "Take Profit 3 (TP3)", value=0.0, min_value=0.0, format="%f")
        with col_add3:
            commission = st.number_input(
                "Комиссия", value=0.0, min_value=0.0, format="%f")
            commission_asset = st.text_input(
                "Валюта комиссии", value=getattr(config, 'BASE_CURRENCY', 'USD'))

        strategy = st.text_input("Стратегия")
        notes = st.text_area("Заметки")

        submitted = st.form_submit_button("Добавить сделку")
        if submitted:
            if not symbol or amount <= 0 or price <= 0:
                st.error(
                    "❌ Поля 'Символ', 'Количество' и 'Цена' обязательны и должны быть больше нуля.")
                return

            # ИСПРАВЛЕНО: Передаем строку с датой напрямую в log_trade, т.к. он умеет ее парсить
            named_args = {
                'sl': str(sl_price) if sl_price > 0 else None, 'tp1': str(tp1_price) if tp1_price > 0 else None,
                'tp2': str(tp2_price) if tp2_price > 0 else None, 'tp3': str(tp3_price) if tp3_price > 0 else None,
                'fee': str(commission) if commission > 0 else None, 'fee_asset': commission_asset if commission > 0 and commission_asset else None,
                'strat': strategy if strategy else None, 'notes': notes if notes else None,
                'date': trade_date_str if trade_date_str else None
            }
            named_args = {k: v for k, v in named_args.items() if v is not None}

            try:
                _logger.info(
                    f"Попытка ручного логирования сделки из Streamlit: {trade_type} {amount} {symbol} @ {price}")
                success, result_msg_or_id = log_trade(
                    trade_type, symbol, str(amount), str(price),
                    source="Streamlit UI", exchange_position_name=exchange,
                    strategy_position_name=strategy, optional_fields=named_args
                )
                if success:
                    st.success(
                        f"✅ Сделка успешно добавлена! ID: {result_msg_or_id}")
                    time.sleep(2)
                    st.rerun()
                else:
                    st.error(f"❌ Ошибка записи сделки: {result_msg_or_id}")
            except Exception as e:
                st.error(f"❌ Критическая ошибка при записи сделки: {e}")
                _logger.error(
                    f"Критическая ошибка вызова log_trade из Streamlit: {e}", exc_info=True)


def _handle_fund_movement_submission(movement_type, asset, amount, source_name, dest_name, fee_amount, fee_asset, tx_id, date_str, notes):
    """Общая логика для обработки отправки формы движения средств."""
    if not asset or amount <= 0:
        st.error("❌ Поля 'Актив' и 'Сумма' обязательны и должны быть больше нуля.")
        return

    s_name, s_type, d_name, d_type = None, None, None, None
    if movement_type == 'DEPOSIT':
        if not dest_name:
            st.error("❌ Укажите счет назначения.")
            return
        s_name = getattr(
            config, 'DEFAULT_DEPOSIT_SOURCE_NAME', "External Inflow")
        s_type = "EXTERNAL"
        d_name = dest_name
        d_type = _determine_entity_type(d_name)
    elif movement_type == 'WITHDRAWAL':
        if not source_name:
            st.error("❌ Укажите счет списания.")
            return
        s_name = source_name
        s_type = _determine_entity_type(s_name)
        d_name = getattr(
            config, 'DEFAULT_WITHDRAW_DESTINATION_NAME', "External Outflow")
        d_type = "EXTERNAL"
    elif movement_type == 'TRANSFER':
        if not source_name or not dest_name:
            st.error("❌ Укажите оба счета для перевода.")
            return
        s_name = source_name
        s_type = _determine_entity_type(s_name)
        d_name = dest_name
        d_type = _determine_entity_type(d_name)

    # ИСПРАВЛЕНО: Используем существующий парсер из utils.py для единообразия
    try:
        movement_timestamp = utils.parse_datetime_from_args(
            {'date': date_str}) if date_str else None
    except ValueError as e:
        st.error(f"❌ {e}")
        return

    try:
        _logger.info(
            f"Попытка ручного логирования движения средств из Streamlit: {movement_type} {amount} {asset}")
        success, result_msg_or_id = log_fund_movement(
            movement_type=movement_type, asset=asset.strip(), amount_str=str(amount),
            source_entity_type=s_type, source_name=s_name, destination_entity_type=d_type, destination_name=d_name,
            fee_amount_str=str(fee_amount) if fee_amount > 0 else None,
            fee_asset=fee_asset.strip() if fee_amount > 0 else None,
            transaction_id_blockchain=tx_id if tx_id else None, notes=notes if notes else None,
            movement_timestamp_obj=movement_timestamp
        )
        if success:
            st.success(
                f"✅ Операция '{movement_type}' успешно добавлена! ID: {result_msg_or_id}")
            time.sleep(2)
            st.rerun()
        else:
            st.error(f"❌ Ошибка записи операции: {result_msg_or_id}")
    except Exception as e:
        st.error(f"❌ Критическая ошибка при записи операции: {e}")
        _logger.error(
            f"Критическая ошибка вызова log_fund_movement из Streamlit: {e}", exc_info=True)


def display_fund_movement_forms():
    """Отображает нужную форму в зависимости от выбора в selectbox."""
    st.subheader("Параметры операции")
    movement_type = st.selectbox("Тип операции", [
                                 "DEPOSIT", "WITHDRAWAL", "TRANSFER"], key="movement_type_selector")

    # Общие поля для всех форм
    common_notes = "Заметки по операции"
    common_date_placeholder = "Пусто = текущее время"

    if movement_type == 'DEPOSIT':
        with st.form(key="deposit_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            asset = col1.text_input("Актив (например, USDT)")
            amount = col2.number_input(
                "Сумма", min_value=0.0, step=0.01, format="%f")
            dest_name = st.text_input(
                "Счет назначения (КУДА)", placeholder="например, Bybit")
            st.divider()
            st.subheader("Дополнительные параметры (опционально)")
            col_add1, col_add2, col_add3 = st.columns(3)
            fee_amount = col_add1.number_input(
                "Комиссия", value=0.0, min_value=0.0, format="%f")
            fee_asset = col_add1.text_input("Валюта комиссии", value=(
                asset or getattr(config, 'BASE_CURRENCY', 'USD')))
            tx_id = col_add2.text_input("ID транзакции (Tx ID)")
            date_str = col_add3.text_input(
                "Дата", placeholder=common_date_placeholder)
            notes = st.text_area(common_notes)
            submitted = st.form_submit_button("Добавить депозит")
            if submitted:
                _handle_fund_movement_submission(
                    "DEPOSIT", asset, amount, None, dest_name, fee_amount, fee_asset, tx_id, date_str, notes)

    elif movement_type == 'WITHDRAWAL':
        with st.form(key="withdrawal_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            asset = col1.text_input("Актив (например, USDT)")
            amount = col2.number_input(
                "Сумма", min_value=0.0, step=0.01, format="%f")
            source_name = st.text_input(
                "Счет списания (ОТКУДА)", placeholder="например, Bybit")
            st.divider()
            st.subheader("Дополнительные параметры (опционально)")
            col_add1, col_add2, col_add3 = st.columns(3)
            fee_amount = col_add1.number_input(
                "Комиссия", value=0.0, min_value=0.0, format="%f")
            fee_asset = col_add1.text_input("Валюта комиссии", value=(
                asset or getattr(config, 'BASE_CURRENCY', 'USD')))
            tx_id = col_add2.text_input("ID транзакции (Tx ID)")
            date_str = col_add3.text_input(
                "Дата", placeholder=common_date_placeholder)
            notes = st.text_area(common_notes)
            submitted = st.form_submit_button("Добавить снятие")
            if submitted:
                _handle_fund_movement_submission(
                    "WITHDRAWAL", asset, amount, source_name, None, fee_amount, fee_asset, tx_id, date_str, notes)

    elif movement_type == 'TRANSFER':
        with st.form(key="transfer_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            asset = col1.text_input("Актив (например, USDT)")
            amount = col2.number_input(
                "Сумма", min_value=0.0, step=0.01, format="%f")
            st.subheader("Источники и назначения")
            scol1, scol2 = st.columns(2)
            source_name = scol1.text_input(
                "Счет списания (ОТКУДА)", placeholder="например, Bybit")
            dest_name = scol2.text_input(
                "Счет назначения (КУДА)", placeholder="например, Binance")
            st.divider()
            st.subheader("Дополнительные параметры (опционально)")
            col_add1, col_add2, col_add3 = st.columns(3)
            fee_amount = col_add1.number_input(
                "Комиссия", value=0.0, min_value=0.0, format="%f")
            fee_asset = col_add1.text_input("Валюта комиссии", value=(
                asset or getattr(config, 'BASE_CURRENCY', 'USD')))
            tx_id = col_add2.text_input("ID транзакции (Tx ID)")
            date_str = col_add3.text_input(
                "Дата", placeholder=common_date_placeholder)
            notes = st.text_area(common_notes)
            submitted = st.form_submit_button("Добавить перевод")
            if submitted:
                _handle_fund_movement_submission(
                    "TRANSFER", asset, amount, source_name, dest_name, fee_amount, fee_asset, tx_id, date_str, notes)


# --- ГЛАВНАЯ ЧАСТЬ СТРАНИЦЫ ---
st.title("📝 Ручной Ввод Данных")
st.caption(
    "Эта страница предназначена для ручного добавления сделок и финансовых операций в систему.")

tab_trade, tab_movement = st.tabs(["📈 Сделки", "💸 Движения Средств"])

with tab_trade:
    display_manual_trade_entry_form()

with tab_movement:
    display_fund_movement_forms()
