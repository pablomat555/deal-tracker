# deal_tracker/utils.py
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from typing import Optional, Dict, Any

from dateutil.parser import parse as parse_datetime_flexible
import config
from models import TradeData, MovementData # Импортируем модели для создания объектов

logger = logging.getLogger(__name__)


def parse_decimal(value: Any) -> Optional[Decimal]:
    """
    [ИСПРАВЛЕНО] Безопасно преобразует ЛЮБОЕ значение в Decimal.
    Если это уже число, возвращает его. Если текст - обрабатывает.
    """
    if value is None:
        return None
    
    # Если это уже число, просто возвращаем его как Decimal
    if isinstance(value, (Decimal, int, float)):
        return Decimal(value)
    
    # Если это текст, обрабатываем его
    if isinstance(value, str):
        if not value.strip():
            return None
        try:
            # Удаляем пробелы, меняем запятую на точку для унификации
            cleaned_str = value.strip().replace(' ', '').replace(',', '.')
            return Decimal(cleaned_str)
        except InvalidOperation:
            logger.warning(f"Не удалось преобразовать строку '{value}' в Decimal.")
            return None
            
    # Если это какой-то другой тип данных (например, bool), возвращаем None
    logger.warning(f"Неподдерживаемый тип для преобразования в Decimal: {type(value)}")
    return None


def get_current_timezone() -> timezone:
    """Возвращает объект timezone на основе смещения из конфига."""
    return timezone(timedelta(hours=config.TZ_OFFSET_HOURS))


def parse_datetime_from_args(named_args: Dict[str, str]) -> datetime:
    """
    Гибко парсит дату из именованных аргументов команды.
    Если дата не найдена или некорректна, возвращает текущее время с правильным часовым поясом.
    """
    date_str = named_args.get('date')
    target_timezone = get_current_timezone()

    if date_str:
        try:
            # Парсим строку в "наивный" объект datetime
            dt_obj = parse_datetime_flexible(date_str)
            # Применяем часовой пояс
            return dt_obj.replace(tzinfo=target_timezone)
        except ValueError:
            logger.warning(
                f"Не удалось распознать формат даты '{date_str}'. Используется текущее время.")

    return datetime.now(target_timezone)


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

# --- Функции-конструкторы для моделей ---

def create_trade_data_from_raw(
    trade_type: str, exchange: str, symbol: str, amount: Decimal, price: Decimal, timestamp: datetime, **kwargs: Any
) -> TradeData:
    """Создает объект TradeData из сырых данных."""
    import uuid
    trade_id = str(uuid.uuid4())
    total_quote_amount = amount * price
    
    trade = TradeData(
        trade_id=trade_id,
        timestamp=timestamp,
        exchange=exchange.lower(),
        symbol=symbol.upper(),
        trade_type=trade_type.upper(),
        amount=amount,
        price=price,
        total_quote_amount=total_quote_amount,
        notes=kwargs.get('notes'),
        commission=kwargs.get('commission'),
        commission_asset=kwargs.get('commission_asset'),
        order_id=kwargs.get('order_id'),
        sl=kwargs.get('sl'),
        tp1=kwargs.get('tp1'),
        tp2=kwargs.get('tp2'),
        tp3=kwargs.get('tp3')
    )
    return trade

def create_movement_data_from_raw(
    movement_type: str, asset: str, amount: Decimal, timestamp: datetime, **kwargs: Any
) -> MovementData:
    """Создает объект MovementData из сырых данных."""
    import uuid
    movement_id = str(uuid.uuid4())
    source_name = kwargs.get('source_name')
    destination_name = kwargs.get('destination_name')

    movement = MovementData(
        movement_id=movement_id,
        timestamp=timestamp,
        movement_type=movement_type.upper(),
        asset=asset.upper(),
        amount=amount,
        source_name=source_name.lower() if source_name else None,
        destination_name=destination_name.lower() if destination_name else None,
        fee_amount=kwargs.get('fee_amount'),
        fee_asset=kwargs.get('fee_asset'),
        notes=kwargs.get('notes'),
        transaction_id_blockchain=kwargs.get('transaction_id_blockchain')
    )
    return movement
