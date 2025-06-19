# deal_tracker/utils.py
"""
Универсальный модуль со вспомогательными функциями для всего проекта.
Содержит утилиты для парсинга, форматирования и загрузки данных.
"""
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from typing import Optional, Dict, Any, List

import streamlit as st
from dateutil.parser import parse as parse_datetime_flexible

import config
import sheets_service
from models import TradeData, MovementData, PositionData, BalanceData, FifoLogData

logger = logging.getLogger(__name__)

# --- Утилиты для Backend (Telegram Bot) ---


def parse_decimal(value_str: Optional[str]) -> Optional[Decimal]:
    """
    Безопасно преобразует строку в Decimal.
    Понимает как точку, так и запятую в качестве разделителя.
    Используется для парсинга ввода от пользователя.
    """
    if value_str is None or not value_str.strip():
        return None
    try:
        cleaned_str = value_str.strip().replace(',', '.')
        return Decimal(cleaned_str)
    except InvalidOperation:
        logger.warning(
            f"Не удалось преобразовать строку '{value_str}' в Decimal.")
        return None


def parse_datetime_from_args(named_args: Dict[str, str]) -> datetime:
    """
    Гибко парсит дату из именованных аргументов команды.
    Если дата не найдена, возвращает текущее время.
    """
    date_str = named_args.get('date')
    target_timezone = timezone(timedelta(hours=config.TZ_OFFSET_HOURS))

    if date_str:
        try:
            dt_obj = parse_datetime_flexible(date_str)
            return dt_obj.astimezone(target_timezone) if dt_obj.tzinfo else dt_obj.replace(tzinfo=target_timezone)
        except ValueError:
            logger.warning(
                f"Не удалось распознать формат даты '{date_str}'. Используется текущее время.")

    return datetime.now(timezone.utc).astimezone(target_timezone)


def determine_entity_type(name: str) -> str:
    """Определяет тип сущности (биржа, кошелек) по имени."""
    if not name:
        return "EXTERNAL"
    name_lower = name.strip().lower()
    if name_lower in [exch.lower() for exch in config.KNOWN_EXCHANGES]:
        return "EXCHANGE"
    if name_lower in [w.lower() for w in config.KNOWN_WALLETS]:
        return "WALLET"
    return "EXTERNAL"


# --- Утилиты для Frontend (Streamlit Dashboard) ---

def format_number(value: Any, precision_str: str = "0.01", add_plus_sign: bool = False, currency_symbol: str = "") -> str:
    """
    Форматирует число в красивую строку с пробелами как разделителем тысяч.
    Пример: 12345.67 -> "12 345.67"
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
        # Более надежный парсинг, сначала пробуем напрямую
        val_decimal = Decimal(val)
    except (InvalidOperation, TypeError, ValueError):
        try:
            # Если не вышло, пробуем очистить строку
            s_val = str(val).replace(' ', '').replace(',', '.')
            val_decimal = Decimal(s_val)
        except (InvalidOperation, TypeError, ValueError):
            return ''  # Если ничего не помогло, стиля нет

    if val_decimal > 0:
        return 'color: #16A34A;'  # Зеленый
    elif val_decimal < 0:
        return 'color: #DC2626;'  # Красный
    return 'color: #6B7280;'  # Серый


@st.cache_data(ttl=300)
def load_all_dashboard_data() -> Dict[str, List[Any]]:
    """
    Централизованно загружает все данные для дэшборда, используя новый sheets_service.
    Результат кэшируется Streamlit'ом на 5 минут.
    """
    logger.info("Загрузка всех данных для дэшборда...")
    # ИСПРАВЛЕНО: Все вызовы приведены в соответствие с финальной версией sheets_service
    # Теперь каждая функция возвращает список типизированных объектов.
    data = {
        'open_positions': sheets_service.get_all_open_positions(),
        'core_trades': sheets_service.get_all_core_trades(),
        'fifo_logs': sheets_service.get_all_fifo_logs(),
        'fund_movements': sheets_service.get_all_fund_movements(),
        'account_balances': sheets_service.get_all_balances(),
        # 'analytics_history' будет генерироваться на лету или браться из спец. лога, пока убираем
    }
    logger.info("Данные для дэшборда успешно загружены.")
    return data
