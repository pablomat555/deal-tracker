# pages/4_Ручной_Ввод.py
import streamlit as st
import logging
import time
from decimal import Decimal
from datetime import datetime, time as dt_time, timezone, timedelta
import os
import sys

# --- Настройка путей и импортов ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from deal_tracker.locales import t
from deal_tracker import config
from deal_tracker.trade_logger import log_trade, log_fund_movement
from deal_tracker import utils
from deal_tracker import sheets_service

# --- НАСТРОЙКА СТРАНИЦЫ ---
st.set_page_config(layout="wide", page_title=t("page_manual_entry_title"))
st.title("📝 " + t("page_manual_entry_header"))

# --- ОБЩИЕ ВИДЖЕТЫ В БОКОВОЙ ПАНЕЛИ ---
with st.sidebar:
    lang_options = ["ru", "en"]
    current_lang = st.session_state.get("lang", "ru")
    lang_index = lang_options.index(current_lang) if current_lang in lang_options else 0
    lang = st.radio(
        "🌐 Язык / Language",
        options=lang_options,
        index=lang_index,
        key='lang_radio_manual_entry' # Уникальный ключ для этой страницы
    )
    st.session_state["lang"] = lang
    st.divider()
    
    st.number_input(
        label=t('timezone_setting_label'),
        min_value=-12, max_value=14,
        value=st.session_state.get('tz_offset', config.TZ_OFFSET_HOURS),
        key='tz_offset',
        help=t('timezone_setting_help')
    )
    
    st.divider()
    if st.button(t('update_button'), key="manual_entry_refresh"):
        st.cache_data.clear()
        sheets_service.invalidate_cache()
        st.rerun()

# --- ОСНОВНЫЕ ФУНКЦИИ ---

def get_current_time_in_user_tz() -> datetime:
    """Возвращает текущее время в выбранной пользователем временной зоне."""
    user_tz_offset = st.session_state.get('tz_offset', config.TZ_OFFSET_HOURS)
    target_timezone = timezone(timedelta(hours=user_tz_offset))
    return datetime.now(timezone.utc).astimezone(target_timezone)

def display_manual_trade_form():
    """
    [ИСПРАВЛЕНО] Отображает форму для ручного ввода сделки, 
    объединяя поля SL/TP и комиссии.
    """
    st.subheader("📈 " + t("add_trade_subheader"))
    with st.form(key="manual_trade_form", clear_on_submit=True):
        col1, col2, col3 = st.columns(3)
        now_in_user_tz = get_current_time_in_user_tz()
        with col1:
            trade_type = st.radio(t("col_type"), ["BUY", "SELL"], horizontal=True)
            symbol = st.text_input(t("col_symbol_placeholder"), "").upper()
            exchange = st.selectbox(t("col_exchange"), config.KNOWN_EXCHANGES)
        with col2:
            amount_str = st.text_input(t("col_qty"))
            price_str = st.text_input(t("col_price"))
        with col3:
            trade_date = st.date_input(t("col_date"), value=now_in_user_tz)
            trade_time = st.time_input(t("col_time"), value=now_in_user_tz.time())

        # --- [ОБЪЕДИНЕННЫЙ БЛОК] Все дополнительные параметры ---
        with st.expander(t("optional_params_expander")):
            # Поля для Stop Loss и Take Profit
            st.markdown("##### Stop Loss / Take Profit")
            scol1, scol2, scol3, scol4 = st.columns(4)
            sl_str = scol1.text_input(t("col_sl"))
            tp1_str = scol2.text_input(t("col_tp1"))
            tp2_str = scol3.text_input(t("col_tp2"))
            tp3_str = scol4.text_input(t("col_tp3"))
            
            st.divider()
            
            # Поля для комиссии и заметок
            st.markdown("##### Комиссия и заметки")
            ccol1, ccol2 = st.columns(2)
            commission_str = ccol1.text_input(t("col_commission"))
            commission_asset_str = ccol2.text_input(t("col_commission_asset"), value=symbol.split('/')[-1] if '/' in symbol else config.BASE_CURRENCY)
            notes = st.text_area(t("col_notes"))

        submitted = st.form_submit_button(t("add_trade_button"))

        if submitted:
            amount_dec = utils.parse_decimal(amount_str)
            price_dec = utils.parse_decimal(price_str)
            if not all([symbol, amount_dec, price_dec]) or amount_dec <= 0 or price_dec <= 0:
                st.error(t("error_fields_required")); return
            
            timestamp = datetime.combine(trade_date, trade_time).replace(tzinfo=now_in_user_tz.tzinfo)
            
            # [ИСПРАВЛЕНО] Собираем ВСЕ опциональные поля в kwargs
            kwargs = {
                'notes': notes if notes else None,
                'sl': utils.parse_decimal(sl_str),
                'tp1': utils.parse_decimal(tp1_str),
                'tp2': utils.parse_decimal(tp2_str),
                'tp3': utils.parse_decimal(tp3_str),
                'commission': utils.parse_decimal(commission_str),
                'commission_asset': commission_asset_str if utils.parse_decimal(commission_str) else None
            }
            # Очищаем kwargs от всех пустых значений
            kwargs = {k: v for k, v in kwargs.items() if v is not None}

            with st.spinner(t("processing")):
                success, msg = log_trade(
                    trade_type=trade_type, exchange=exchange, symbol=symbol,
                    amount=amount_dec, price=price_dec, timestamp=timestamp, **kwargs
                )
            if success:
                st.success(t("success_trade_added") + f" ID: {msg}"); st.balloons(); time.sleep(2); st.rerun()
            else:
                st.error(f"❌ {t('error_generic')}: {msg}")


def display_manual_movement_forms():
    """Отображает формы для ввода/вывода/перевода средств."""
    st.subheader("💸 " + t("add_movement_subheader"))
    movement_type = st.selectbox(t("col_type"), ["DEPOSIT", "WITHDRAWAL", "TRANSFER"])

    def handle_submission(m_type, asset, amount_str, source, dest, date, time_val, notes):
        amount = utils.parse_decimal(amount_str)
        if not all([asset, amount]) or amount <= 0:
            st.error(t("error_asset_amount_required")); return

        now_in_user_tz = get_current_time_in_user_tz()
        timestamp = datetime.combine(date, time_val).replace(tzinfo=now_in_user_tz.tzinfo)
        kwargs = {'notes': notes}

        with st.spinner(t("processing")):
            success, msg = log_fund_movement(
                movement_type=m_type, asset=asset, amount=amount, timestamp=timestamp,
                source_name=source, destination_name=dest, **kwargs
            )
        if success:
            st.success(t("success_movement_added").format(m_type=m_type) + f" ID: {msg}"); st.balloons(); time.sleep(2); st.rerun()
        else:
            st.error(f"❌ {t('error_generic')}: {msg}")

    with st.form(key=f"{movement_type}_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        asset = col1.text_input(t("col_asset_placeholder"), key=f"asset_{movement_type}").upper()
        amount_str = col2.text_input(t("col_amount"), key=f"amount_{movement_type}")
        
        source, dest = None, None
        if movement_type == "DEPOSIT":
            dest = st.selectbox(t("col_destination_account"), config.KNOWN_EXCHANGES + config.KNOWN_WALLETS, key="dest_dep")
        elif movement_type == "WITHDRAWAL":
            source = st.selectbox(t("col_source_account"), config.KNOWN_EXCHANGES + config.KNOWN_WALLETS, key="source_with")
        elif movement_type == "TRANSFER":
            c1, c2 = st.columns(2)
            source = c1.selectbox(t("col_source_account"), config.KNOWN_EXCHANGES + config.KNOWN_WALLETS, key="source_trans")
            dest = c2.selectbox(t("col_destination_account"), config.KNOWN_EXCHANGES + config.KNOWN_WALLETS, key="dest_trans")

        col_date, col_time = st.columns(2)
        now_in_user_tz = get_current_time_in_user_tz()
        trade_date = col_date.date_input(t("col_date"), value=now_in_user_tz, key=f"date_{movement_type}")
        trade_time = col_time.time_input(t("col_time"), value=now_in_user_tz.time(), key=f"time_{movement_type}")

        notes = st.text_area(t("col_notes"), key=f"notes_{movement_type}")
        submitted = st.form_submit_button(t("add_button") + f" {movement_type.lower()}")
        if submitted:
            handle_submission(movement_type, asset, amount_str, source, dest, trade_date, trade_time, notes)

# --- ГЛАВНЫЙ КОД ---
tab_trade, tab_movement = st.tabs([t("tab_trades"), t("tab_movements")])

with tab_trade:
    display_manual_trade_form()

with tab_movement:
    display_manual_movement_forms()