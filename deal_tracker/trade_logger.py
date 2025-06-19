# deal_tracker/trade_logger.py
import uuid
from datetime import datetime
import logging
from decimal import Decimal
from typing import List, Optional, Tuple, Dict, Any

import sheets_service
import config
from models import TradeData, MovementData, PositionData, BalanceData

logger = logging.getLogger(__name__)

# --- Вспомогательные функции, работающие с МОДЕЛЯМИ ---


def _find_balance(account_name: str, asset: str, all_balances: List[BalanceData]) -> Optional[BalanceData]:
    """Находит объект баланса в списке."""
    account_name_lower = account_name.lower()
    asset_upper = asset.upper()
    for balance in all_balances:
        if balance.account_name.lower() == account_name_lower and balance.asset.upper() == asset_upper:
            return balance
    return None


def _has_sufficient_balance(account_name: str, asset: str, required_amount: Decimal, all_balances: List[BalanceData]) -> bool:
    """Проверяет достаточность баланса, работая со списком моделей."""
    balance_obj = _find_balance(account_name, asset, all_balances)
    current_balance = balance_obj.balance if balance_obj and balance_obj.balance is not None else Decimal(
        '0')

    is_sufficient = current_balance >= required_amount
    if not is_sufficient:
        logger.warning(
            f"Проверка баланса: НЕ ОК. {account_name}/{asset}. Требуется: {required_amount}, Доступно: {current_balance}")
    return is_sufficient


def _find_position(symbol: str, exchange: str, all_positions: List[PositionData]) -> Optional[PositionData]:
    """Находит объект открытой позиции в списке."""
    symbol_upper = symbol.upper()
    exchange_lower = exchange.lower()
    for pos in all_positions:
        if pos.symbol.upper() == symbol_upper and pos.exchange.lower() == exchange_lower:
            return pos
    return None

# --- Основная логика ---


def log_trade(
    trade_type: str,
    exchange: str,
    symbol: str,
    amount: Decimal,
    price: Decimal,
    timestamp: datetime,
    **kwargs: Any
) -> Tuple[bool, str]:
    """
    Логирует торговую операцию. Принимает чистые, типизированные данные.
    Оркестрирует процесс: проверка балансов, запись сделки, обновление балансов и позиций.
    """
    trade_id = str(uuid.uuid4())
    log_context = f"TradeID: {trade_id}, {trade_type} {amount} {symbol} @ {price} on {exchange}"
    logger.info(f"Начало логирования сделки. {log_context}")

    base_asset, quote_asset = symbol.upper().split('/')
    exchange_lower = exchange.lower()

    # 1. Создание объекта сделки
    trade = TradeData(
        trade_id=trade_id,
        timestamp=timestamp,
        exchange=exchange_lower,
        symbol=symbol.upper(),
        trade_type=trade_type.upper(),
        amount=amount,
        price=price,
        total_quote_amount=(amount * price),
        notes=kwargs.get('notes'),
        commission=kwargs.get('commission'),
        commission_asset=kwargs.get('commission_asset'),
        order_id=kwargs.get('order_id')
    )

    # 2. Проверка балансов
    all_balances = sheets_service.get_all_balances()
    balance_changes: List[Dict[str, Any]] = []

    if trade.trade_type == 'BUY':
        required_quote = trade.total_quote_amount
        if trade.commission and trade.commission_asset and trade.commission_asset.upper() == quote_asset:
            required_quote += trade.commission

        if not _has_sufficient_balance(exchange_lower, quote_asset, required_quote, all_balances):
            return False, f"Недостаточно {quote_asset} на счете {exchange}."

        balance_changes.append(
            {'account': exchange_lower, 'asset': quote_asset, 'change': -required_quote})
        balance_changes.append(
            {'account': exchange_lower, 'asset': base_asset, 'change': trade.amount})

        if trade.commission and trade.commission_asset and trade.commission_asset.upper() != quote_asset:
            if not _has_sufficient_balance(exchange_lower, trade.commission_asset, trade.commission, all_balances):
                return False, f"Недостаточно {trade.commission_asset} для комиссии."
            balance_changes.append(
                {'account': exchange_lower, 'asset': trade.commission_asset, 'change': -trade.commission})

    elif trade.trade_type == 'SELL':
        if not _has_sufficient_balance(exchange_lower, base_asset, trade.amount, all_balances):
            return False, f"Недостаточно {base_asset} на счете {exchange}."

        balance_changes.append(
            {'account': exchange_lower, 'asset': base_asset, 'change': -trade.amount})
        balance_changes.append(
            {'account': exchange_lower, 'asset': quote_asset, 'change': trade.total_quote_amount})
        # Обработка комиссии при продаже (если она взимается отдельно) будет здесь

    # 3. Запись основной транзакции
    if not sheets_service.add_trade(trade):
        return False, "Ошибка записи сделки в Core_Trades."

    # 4. Обновление балансов
    if not sheets_service.batch_update_balances(balance_changes):
        logger.critical(
            f"ТРЕБУЕТСЯ РУЧНОЕ ВМЕШАТЕЛЬСТВО! Сделка {trade_id} записана, но балансы НЕ обновлены!")
        return False, "Критическая ошибка: балансы не обновлены после записи сделки."

    # 5. Синхронизация открытых позиций
    _sync_open_position(trade)

    logger.info(f"Сделка успешно залогирована. {log_context}")
    return True, trade_id


def _sync_open_position(trade: TradeData):
    """Обновляет, создает или удаляет запись в Open_Positions на основе сделки."""
    all_positions = sheets_service.get_all_open_positions()
    existing_pos = _find_position(trade.symbol, trade.exchange, all_positions)

    # Получаем финальный баланс базового актива после всех изменений
    final_balances = sheets_service.get_all_balances()
    final_base_asset_balance_obj = _find_balance(
        trade.exchange, trade.symbol.split('/')[0], final_balances)
    final_net_amount = final_base_asset_balance_obj.balance if final_base_asset_balance_obj else Decimal(
        '0')

    # Порог для определения "нулевого" баланса
    zero_threshold = Decimal('1e-8')

    if final_net_amount <= zero_threshold:
        # Удаляем позицию, если она есть и баланс стал нулевым
        if existing_pos and existing_pos.row_number:
            logger.info(
                f"Закрытие позиции {trade.symbol} на {trade.exchange} (баланс {final_net_amount}). Удаление строки {existing_pos.row_number}.")
            sheets_service.delete_row(
                config.OPEN_POSITIONS_SHEET_NAME, existing_pos.row_number)
        return

    # Если баланс не нулевой, обновляем или создаем позицию
    new_avg_price = trade.price
    if trade.trade_type == 'BUY':
        if existing_pos and existing_pos.net_amount > 0:
            # Пересчитываем среднюю цену
            old_total_value = existing_pos.net_amount * existing_pos.avg_entry_price
            new_total_value = trade.amount * trade.price
            new_avg_price = (old_total_value +
                             new_total_value) / final_net_amount

        position_to_save = PositionData(
            row_number=existing_pos.row_number if existing_pos else None,
            symbol=trade.symbol, exchange=trade.exchange,
            net_amount=final_net_amount, avg_entry_price=new_avg_price,
            last_updated=datetime.now()
        )

        if existing_pos:
            logger.info(
                f"Обновление позиции BUY для {trade.symbol}. Новая средняя: {new_avg_price}")
            sheets_service.update_position(position_to_save)
        else:
            logger.info(f"Создание новой позиции BUY для {trade.symbol}.")
            sheets_service.add_position(position_to_save)

    elif trade.trade_type == 'SELL':
        if existing_pos:
            # При продаже средняя цена не меняется
            existing_pos.net_amount = final_net_amount
            existing_pos.last_updated = datetime.now()
            logger.info(
                f"Обновление позиции SELL для {trade.symbol}. Новый объем: {final_net_amount}")
            sheets_service.update_position(existing_pos)
        else:
            # Эта ситуация не должна возникать при корректной логике, но для надежности
            logger.error(
                f"Продажа {trade.symbol} без существующей открытой позиции. Позиция не создана.")


def log_fund_movement(
    movement_type: str,
    asset: str,
    amount: Decimal,
    timestamp: datetime,
    **kwargs: Any
) -> Tuple[bool, str]:
    """Логирует движение средств (депозит, снятие, перевод)."""
    movement_id = str(uuid.uuid4())
    log_context = f"MoveID: {movement_id}, {movement_type} {amount} {asset}"
    logger.info(f"Начало логирования движения средств. {log_context}")

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

    # Проверка баланса для снятия или перевода
    if movement.movement_type in ['WITHDRAWAL', 'TRANSFER'] and movement.source_name:
        all_balances = sheets_service.get_all_balances()
        if not _has_sufficient_balance(movement.source_name, movement.asset, movement.amount, all_balances):
            return False, f"Недостаточно {movement.asset} на счете {movement.source_name}."

    if not sheets_service.add_movement(movement):
        return False, "Ошибка записи движения средств."

    # Обновление балансов
    balance_changes = []
    if movement.source_name:
        balance_changes.append({'account': movement.source_name,
                               'asset': movement.asset, 'change': -movement.amount})
    if movement.destination_name:
        balance_changes.append({'account': movement.destination_name,
                               'asset': movement.asset, 'change': movement.amount})

    if balance_changes:
        if not sheets_service.batch_update_balances(balance_changes):
            logger.critical(
                f"ТРЕБУЕТСЯ РУЧНОЕ ВМЕШАТЕЛЬСТВО! Движение {movement_id} записано, но балансы НЕ обновлены!")
            return False, "Критическая ошибка: балансы не обновлены после записи движения."

    logger.info(f"Движение средств успешно залогировано. {log_context}")
    return True, movement_id
