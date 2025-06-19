# deal_tracker/dashboard_utils.py
"""
Вспомогательные утилиты, предназначенные ИСКЛЮЧИТЕЛЬНО
для использования в Streamlit-дэшборде.
"""
import logging
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List

import streamlit as st
import sheets_service
import config
from models import AnalyticsData  # Импортируем модель для истории аналитики

logger = logging.getLogger(__name__)


def format_number(value: Any, precision_str: str = "0.01", add_plus_sign: bool = False, currency_symbol: str = "") -> str:
    """
    Форматирует число в красивую строку с пробелами как разделителем тысяч.
    """
    try:
        val = Decimal(value)
        decimals = abs(Decimal(precision_str).as_tuple().exponent)
        formatted_str = f"{val:,.{decimals}f}".replace(',', ' ')
        if add_plus_sign and val > 0:
            formatted_str = f"+{formatted_str}"
        if currency_symbol:
            formatted_str = f"{formatted_str} {currency_symbol}"
        return formatted_str
    except (InvalidOperation, TypeError, ValueError):
        return "-"


def style_pnl_value(val: Any) -> str:
    """Возвращает CSS-стиль для ячеек PNL в зависимости от значения."""
    try:
        val_decimal = Decimal(val)
    except (InvalidOperation, TypeError, ValueError):
        try:
            s_val = str(val).replace(' ', '').replace(',', '.')
            val_decimal = Decimal(s_val)
        except (InvalidOperation, TypeError, ValueError):
            return ''

    if val_decimal > 0:
        return 'color: #16A34A;'
    elif val_decimal < 0:
        return 'color: #DC2626;'
    return 'color: #6B7280;'


@st.cache_data(ttl=300)
def load_all_dashboard_data() -> Dict[str, List[Any]]:
    """
    Централизованно загружает все данные для дэшборда, используя новый sheets_service.
    Результат кэшируется Streamlit'ом на 5 минут.
    """
    logger.info("Загрузка всех данных для дэшборда...")
    data = {
        'analytics_history': sheets_service.get_all_records(config.ANALYTICS_SHEET_NAME, AnalyticsData),
        'open_positions': sheets_service.get_all_open_positions(),
        'core_trades': sheets_service.get_all_core_trades(),
        'fifo_logs': sheets_service.get_all_fifo_logs(),
        'fund_movements': sheets_service.get_all_fund_movements(),
        'account_balances': sheets_service.get_all_balances(),
    }
    logger.info("Данные для дэшборда успешно загружены.")
    return data
