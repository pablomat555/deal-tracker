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

# [ИСПРАВЛЕНО] Импортируем модуль целиком, а не только функцию t
from deal_tracker import locales
from deal_tracker import config, utils, sheets_service
from deal_tracker.trade_logger import log_trade, log_fund_movement


# --- НАСТРОЙКА СТРАНИЦЫ ---
st.set_page_config(layout="wide", page_title=locales.t("page_manual_entry_title"))
st.title("📝 " + locales.t("page_manual_entry_header"))
logger = logging.getLogger(__name__)

# --- ОБЩИЕ ВИДЖЕТЫ В БОКОВОЙ ПАНЕЛИ ---
with st.sidebar:
    # [ИСПРАВЛЕНО] Проверяем наличие функции в самом модуле locales
    if hasattr(locales, 'render_language_selector'):
         locales.render_language_selector()
    else: # Обратная совместимость, если функция еще не определена
        lang_options = ["ru", "en"]
        current_lang = st.session_state.get("lang", "ru")
        lang_index = lang_options.index(current_lang) if current_lang in lang_options else 0
        lang = st.radio(locales.t('language_selector_label'), options=lang_options, index=lang_index, key='lang_radio_manual_entry', horizontal=True)
        st.session_state["lang"] = lang
    
    st.divider()
    st.number_input(
        label=locales.t('timezone_setting_label'),
        min_value=-12, max_value=14,
        value=st.session_state.get('tz_offset', config.TZ_OFFSET_HOURS),
        key='tz_offset',
        help=locales.t('timezone_setting_help')
    )
    st.divider()
    if st.button(locales.t('update_button'), key="manual_entry_refresh"):
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
    """Отображает улучшенную форму для ручного ввода сделки с динамическими обновлениями."""
    st.subheader("📈 " + locales.t("add_trade_subheader"))

    symbol = st.text_input(locales.t("col_symbol_placeholder"), key="symbol_input").upper()

    quote_asset = symbol.split('/')[-1] if '/' in symbol else config.BASE_CURRENCY
    base_asset = symbol.split('/')[0] if '/' in symbol else None
    potential_assets = [quote_asset, base_asset, "BNB", "KCS", "OKB", "USDT"]
    fee_asset_options = sorted(list(set(asset for asset in potential_assets if asset)))

    with st.form(key="manual_trade_form", clear_on_submit=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            trade_type = st.radio(locales.t("col_type"), ["BUY", "SELL"], horizontal=True)
            exchange = st.selectbox(locales.t("col_exchange"), config.KNOWN_EXCHANGES)
        with col2:
            amount_str = st.text_input(locales.t("col_qty"))
            price_str = st.text_input(locales.t("col_price"))
        with col3:
            now_in_user_tz = get_current_time_in_user_tz()
            trade_date = st.date_input(locales.t("col_date"), value=now_in_user_tz)
            trade_time = st.time_input(locales.t("col_time"), value=now_in_user_tz.time())

        with st.expander(locales.t("optional_params_expander")):
            notes = st.text_area(locales.t("col_notes"))
            st.markdown("---")
            st.write(f"**Stop Loss / Take Profit**")
            scol1, scol2, scol3, scol4 = st.columns(4)
            sl_str, tp1_str, tp2_str, tp3_str = scol1.text_input(locales.t("col_sl")), scol2.text_input(locales.t("col_tp1")), scol3.text_input(locales.t("col_tp2")), scol4.text_input(locales.t("col_tp3"))
            st.markdown("---")
            st.write(f"**{locales.t('fee_config_header')}**")
            fee_type = st.radio(locales.t('fee_type_label'), [locales.t('fee_type_percent'), locales.t('fee_type_fixed')], horizontal=True, key="fee_type_radio")
            fcol1, fcol2 = st.columns(2)
            with fcol1:
                if fee_type == locales.t('fee_type_percent'):
                    fee_percent_str = st.text_input(locales.t('fee_percent_label'), "0.1", key="fee_percent_input")
                    commission_str = None
                else:
                    commission_str = st.text_input(locales.t('fee_fixed_amount_label'), key="fee_fixed_input")
                    fee_percent_str = None
            with fcol2:
                commission_asset_str = st.selectbox(locales.t("col_commission_asset"), fee_asset_options, key="fee_asset_select")

        if st.form_submit_button(locales.t("add_trade_button")):
            amount_dec, price_dec = utils.parse_decimal(amount_str), utils.parse_decimal(price_str)
            if not all([symbol, amount_dec, price_dec]) or amount_dec <= 0 or price_dec <= 0:
                st.error(locales.t("error_fields_required")); return
            
            commission_dec = None
            if fee_percent_str:
                if fee_percent := utils.parse_decimal(fee_percent_str):
                    commission_dec = (amount_dec * price_dec) * (fee_percent / Decimal(100))
            elif commission_str:
                commission_dec = utils.parse_decimal(commission_str)

            timestamp = datetime.combine(trade_date, trade_time).replace(tzinfo=now_in_user_tz.tzinfo)
            kwargs = {k: v for k, v in {'notes': notes, 'sl': utils.parse_decimal(sl_str), 'tp1': utils.parse_decimal(tp1_str), 'tp2': utils.parse_decimal(tp2_str), 'tp3': utils.parse_decimal(tp3_str), 'commission': commission_dec, 'commission_asset': commission_asset_str if commission_dec else None}.items() if v is not None}
            with st.spinner(locales.t("processing")):
                success, msg = log_trade(trade_type=trade_type, exchange=exchange, symbol=symbol, amount=amount_dec, price=price_dec, timestamp=timestamp, **kwargs)
            if success:
                st.success(f"{locales.t('success_trade_added')} ID: {msg}"); st.balloons(); time.sleep(2); st.rerun()
            else:
                st.error(f"❌ {locales.t('error_generic')}: {msg}")

def display_manual_movement_forms():
    """Отображает формы для ввода/вывода/перевода средств с возможностью указания комиссии."""
    st.subheader("💸 " + locales.t("add_movement_subheader"))
    movement_type = st.selectbox(locales.t("col_type"), ["DEPOSIT", "WITHDRAWAL", "TRANSFER"], key="movement_type_select")

    with st.form(key=f"{movement_type}_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        asset = col1.text_input(locales.t("col_asset_placeholder"), key=f"asset_{movement_type}").upper()
        amount_str = col2.text_input(locales.t("col_amount"), key=f"amount_{movement_type}")
        
        source, dest = None, None
        if movement_type == "DEPOSIT":
            dest = st.selectbox(locales.t("col_destination_account"), config.KNOWN_EXCHANGES + config.KNOWN_WALLETS, key="dest_dep")
        elif movement_type == "WITHDRAWAL":
            source = st.selectbox(locales.t("col_source_account"), config.KNOWN_EXCHANGES + config.KNOWN_WALLETS, key="source_with")
        elif movement_type == "TRANSFER":
            c1, c2 = st.columns(2)
            source = c1.selectbox(locales.t("col_source_account"), config.KNOWN_EXCHANGES + config.KNOWN_WALLETS, key="source_trans")
            dest = c2.selectbox(locales.t("col_destination_account"), config.KNOWN_EXCHANGES + config.KNOWN_WALLETS, key="dest_trans")

        col_date, col_time = st.columns(2)
        now_in_user_tz = get_current_time_in_user_tz()
        trade_date = col_date.date_input(locales.t("col_date"), value=now_in_user_tz, key=f"date_{movement_type}")
        trade_time = col_time.time_input(locales.t("col_time"), value=now_in_user_tz.time(), key=f"time_{movement_type}")

        with st.expander(locales.t("optional_params_expander")):
            fcol1, fcol2 = st.columns(2)
            fee_amount_str = fcol1.text_input(locales.t('col_commission'), key=f"fee_amount_{movement_type}")
            fee_asset_str = fcol2.text_input(locales.t('col_fee_asset'), key=f"fee_asset_{movement_type}", value=asset)
            notes = st.text_area(locales.t("col_notes"), key=f"notes_{movement_type}")

        if st.form_submit_button(f"{locales.t('add_button')} {movement_type.lower()}"):
            amount = utils.parse_decimal(amount_str)
            if not all([asset, amount]) or amount <= 0:
                st.error(locales.t("error_asset_amount_required")); return
            
            timestamp = datetime.combine(trade_date, trade_time).replace(tzinfo=now_in_user_tz.tzinfo)
            fee_amount = utils.parse_decimal(fee_amount_str)
            kwargs = {k: v for k, v in {'notes': notes, 'fee_amount': fee_amount, 'fee_asset': fee_asset_str if fee_amount else None}.items() if v is not None}
            
            with st.spinner(locales.t("processing")):
                success, msg = log_fund_movement(movement_type=movement_type, asset=asset, amount=amount, timestamp=timestamp, source_name=source, destination_name=dest, **kwargs)
            if success:
                st.success(f"{locales.t('success_movement_added').format(m_type=movement_type)} ID: {msg}"); st.balloons(); time.sleep(2); st.rerun()
            else:
                st.error(f"❌ {locales.t('error_generic')}: {msg}")

# --- ГЛАВНЫЙ КОД ---
tab_trade, tab_movement = st.tabs([locales.t("tab_trades"), locales.t("tab_movements")])

with tab_trade:
    display_manual_trade_form()

with tab_movement:
    display_manual_movement_forms()