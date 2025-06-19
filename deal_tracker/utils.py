# deal_tracker/utils.py
"""
Универсальный модуль со вспомогательными утилитами, не зависящими от фреймворков.
Используется в backend-части (бот, сервисы).
"""
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from typing import Optional, Dict

from dateutil.parser import parse as parse_datetime_flexible
import config

logger = logging.getLogger(__name__)


def parse_decimal(value_str: Optional[str]) -> Optional[Decimal]:
    """
    Безопасно преобразует строку в Decimal, понимая точку и запятую.
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
