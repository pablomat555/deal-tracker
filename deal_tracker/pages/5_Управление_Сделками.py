import streamlit as st
import pandas as pd
import os
import sys
from typing import List, Dict

# --- Настройка путей и импортов ---
# Это гарантирует, что приложение сможет найти ваши модули, даже будучи запущенным из папки pages
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# [ИСПРАВЛЕНО] Импортируем весь модуль `locales`, а не только функцию `t`
from deal_tracker import locales, config, dashboard_utils, sheets_service
from deal_tracker.models import TradeData

# --- НАСТРОЙКА СТРАНИЦЫ ---
# [ИСПРАВЛЕНО] Теперь используем `locales.t()` для получения перевода
st.set_page_config(layout="wide", page_title=locales.t('page_manage_trades_title'))
st.title("🛠️ " + locales.t('page_manage_trades_header'))

# Отрисовка переключателя языка в боковой панели
with st.sidebar:
    # Теперь этот вызов будет работать, так как модуль `locales` импортирован
    locales.render_language_selector()
    st.divider()
    
# --- Управление состоянием ---
if 'trades_to_delete_labels' not in st.session_state:
    st.session_state.trades_to_delete_labels = []

@st.cache_data(ttl=60) # Кэшируем данные на 60 секунд
def load_trades_data() -> List[TradeData]:
    """Загружает и кэширует сделки, чтобы не делать лишних запросов."""
    data, errors = dashboard_utils.load_all_data_with_error_handling()
    if errors:
        with st.expander("⚠️ " + locales.t('error_loading_data_expander'), expanded=True):
            for msg in errors: st.error(msg)
        return []
    return data.get('core_trades', [])

# --- Основная логика страницы ---
core_trades = load_trades_data()

# Кнопка для ручного обновления данных
if st.button("🔄 " + locales.t('refresh_data_button')):
    st.cache_data.clear() # Очищаем кэш Streamlit
    st.rerun()

if not core_trades:
    st.info(locales.t('no_core_trades_to_manage'))
else:
    # --- СОЗДАНИЕ СПИСКА СДЕЛОК ДЛЯ ВЫБОРА ---
    core_trades.sort(key=lambda t: t.timestamp, reverse=True)
    
    trade_options: Dict[str, TradeData] = {}
    for trade in core_trades:
        # Используем f-строки с выравниванием для красивого отображения
        label = (
            f"{trade.timestamp.strftime('%Y-%m-%d %H:%M')} | "
            f"{trade.trade_type.upper():<4} | "
            f"{trade.symbol:<12} | "
            f"{locales.t('col_qty')}: {trade.amount!s:<8} | "
            f"{locales.t('col_price')}: {trade.price!s}"
        )
        trade_options[label] = trade

    st.subheader(locales.t('select_trades_to_delete_header'))
    
    # Используем session_state для хранения выбранных элементов
    selected_trade_labels = st.multiselect(
        label=locales.t('select_trades_to_delete_label'),
        options=list(trade_options.keys()),
        key='trades_to_delete_labels' # Привязываем к состоянию сессии
    )

    if selected_trade_labels:
        st.markdown("---")
        trades_to_delete = [trade_options[label] for label in selected_trade_labels]
        
        # --- Подтверждение удаления ---
        with st.expander("⚠️ " + locales.t('delete_confirmation_header'), expanded=True):
            st.warning(locales.t('delete_warning'))
            
            st.write(locales.t("you_have_selected_for_deletion"))
            # Используем .dict() для pydantic моделей, если они есть, иначе __dict__
            df_to_delete_data = [t.dict() if hasattr(t, 'dict') else t.__dict__ for t in trades_to_delete]
            df_to_delete = pd.DataFrame(df_to_delete_data)
            st.dataframe(df_to_delete[['timestamp', 'trade_type', 'symbol', 'amount', 'price', 'row_number']], use_container_width=True)
            
            # --- Кнопка удаления ---
            if st.button("🔴 " + locales.t('delete_button_confirm')):
                row_numbers_to_delete = [t.row_number for t in trades_to_delete]
                
                with st.spinner(locales.t('deleting_in_progress')):
                    # Используем пакетное удаление
                    success = sheets_service.batch_delete_rows(config.CORE_TRADES_SHEET_NAME, row_numbers_to_delete)
                
                if success:
                    st.success(locales.t('delete_success_message').format(count=len(row_numbers_to_delete)))
                    st.info(locales.t('delete_post_action_info'))
                    st.balloons()
                    # Очищаем выбор после успешного удаления
                    st.session_state.trades_to_delete_labels = []
                    # Очищаем кэш, чтобы при следующем rerun загрузились свежие данные
                    st.cache_data.clear()
                    st.rerun() # Перезапускаем страницу, чтобы обновить список
                else:
                    st.error(locales.t('delete_error_message'))
