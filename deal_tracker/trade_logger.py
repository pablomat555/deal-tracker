# deal_tracker/trade_logger.py
import uuid
import copy  # <-- Добавлен для глубокого копирования
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
    account_name_lower = account_name.lower()
    asset_upper = asset.upper()
    for balance in all_balances:
        if balance.account_name.lower() == account_name_lower and balance.asset.upper() == asset_upper:
            return balance
    return None


def _find_position(symbol: str, exchange: str, all_positions: List[PositionData]) -> Optional[PositionData]:
    symbol_upper = symbol.upper()
    exchange_lower = exchange.lower()
    for pos in all_positions:
        if pos.symbol.upper() == symbol_upper and pos.exchange.lower() == exchange_lower:
            return pos
    return None


# ИСПРАВЛЕНИЕ: Новая функция для обновления балансов в памяти
def _apply_balance_changes_in_memory(
    current_balances: List[BalanceData],
    changes: List[Dict[str, Any]]
) -> List[BalanceData]:
    """
    Применяет изменения к списку балансов в памяти, чтобы избежать race condition.
    Возвращает новый, обновленный список балансов.
    """
    # Используем deepcopy, чтобы не изменять оригинальный список, переданный в другие функции
    updated_balances = copy.deepcopy(current_balances)
    balances_map = {
        (b.account_name.lower(), b.asset.upper()): b for b in updated_balances
    }

    for change in changes:
        account, asset, change_amount = change['account'].lower(
        ), change['asset'].upper(), change['change']
        key = (account, asset)

        if key in balances_map:
            balance_obj = balances_map[key]
            if balance_obj.balance is None:
                balance_obj.balance = Decimal('0')
            balance_obj.balance += change_amount
        else:
            # Если такого баланса не было, создаем новый
            new_balance = BalanceData(
                account_name=account, asset=asset, balance=change_amount, last_updated=datetime.now())
            updated_balances.append(new_balance)
            balances_map[key] = new_balance

    return updated_balances


# --- Основная логика ---

def log_trade(
    trade_type: str, exchange: str, symbol: str, amount: Decimal, price: Decimal, timestamp: datetime, **kwargs: Any
) -> Tuple[bool, str]:
    """
    Оркестрирует логирование сделки с минимальным количеством API-вызовов и надежной проверкой баланса.
    """
    trade_id = str(uuid.uuid4())
    logger.info(
        f"[LOGGER] TradeID: {trade_id}. Начало обработки: {trade_type} {amount} {symbol}")

    try:
        all_balances = sheets_service.get_all_balances()
        all_positions = sheets_service.get_all_open_positions()
    except Exception as e:
        logger.error(
            f"[LOGGER] Не удалось загрузить исходные данные из Sheets: {e}")
        return False, "Ошибка связи с Google Sheets."

    base_asset, quote_asset = symbol.upper().split('/')
    exchange_lower = exchange.lower()

    # Проверка балансов
    if trade_type.upper() == 'BUY':
        required_quote = amount * price
        if kwargs.get('commission') and kwargs.get('commission_asset', '').upper() == quote_asset:
            required_quote += kwargs['commission']
        balance_obj = _find_balance(exchange_lower, quote_asset, all_balances)
        if not balance_obj or balance_obj.balance is None or balance_obj.balance < required_quote:
            current_bal = balance_obj.balance if balance_obj and balance_obj.balance is not None else Decimal(
                '0')
            return False, f"Недостаточно {quote_asset}. Нужно: {required_quote:.2f}, доступно: {current_bal:.2f}."

    elif trade_type.upper() == 'SELL':
        balance_obj = _find_balance(exchange_lower, base_asset, all_balances)
        if not balance_obj or balance_obj.balance is None or balance_obj.balance < amount:
            current_bal = balance_obj.balance if balance_obj and balance_obj.balance is not None else Decimal(
                '0')
            return False, f"Недостаточно {base_asset}. Нужно: {amount}, доступно: {current_bal}."

    # Расчет PNL
    calculated_pnl = None
    if trade_type.upper() == 'SELL':
        existing_pos = _find_position(symbol, exchange_lower, all_positions)
        if existing_pos and existing_pos.avg_entry_price:
            calculated_pnl = (price - existing_pos.avg_entry_price) * amount

    # Формирование объекта сделки и изменений баланса
    trade = TradeData(
        trade_id=trade_id, timestamp=timestamp, exchange=exchange_lower, symbol=symbol.upper(),
        trade_type=trade_type.upper(), amount=amount, price=price, total_quote_amount=(amount * price),
        trade_pnl=calculated_pnl, notes=kwargs.get('notes'), commission=kwargs.get('commission'),
        commission_asset=kwargs.get('commission_asset'), order_id=kwargs.get('order_id')
    )
    balance_changes = _calculate_balance_changes(
        trade, base_asset, quote_asset)

    # Запись в таблицы
    if not sheets_service.add_trade(trade):
        return False, "Ошибка записи сделки в Core_Trades."
    if not sheets_service.batch_update_balances(balance_changes):
        logger.critical(
            f"КРИТИЧЕСКАЯ ПРОВЕРКА! Сделка {trade_id} записана, но балансы НЕ обновлены!")
        return False, "Критическая ошибка: балансы не обновлены."

    # ИСПРАВЛЕНИЕ: Обновляем балансы в памяти перед синхронизацией позиции
    updated_balances = _apply_balance_changes_in_memory(
        all_balances, balance_changes)

    # Передаем в функцию актуальные данные
    _sync_open_position(trade, all_positions, updated_balances)

    logger.info(f"[LOGGER] TradeID: {trade_id}. Сделка успешно залогирована.")
    return True, trade_id


def _calculate_balance_changes(trade: TradeData, base_asset: str, quote_asset: str) -> List[Dict[str, Any]]:
    """Рассчитывает изменения балансов на основе сделки."""
    changes = []
    total_quote = trade.total_quote_amount or Decimal(0)

    if trade.trade_type == 'BUY':
        changes.append({'account': trade.exchange,
                       'asset': quote_asset, 'change': -total_quote})
        changes.append({'account': trade.exchange,
                       'asset': base_asset, 'change': trade.amount})
        if trade.commission and trade.commission_asset:
            if trade.commission_asset.upper() == quote_asset:
                changes[0]['change'] -= trade.commission
            else:
                changes.append(
                    {'account': trade.exchange, 'asset': trade.commission_asset, 'change': -trade.commission})
    elif trade.trade_type == 'SELL':
        changes.append({'account': trade.exchange,
                       'asset': base_asset, 'change': -trade.amount})
        changes.append({'account': trade.exchange,
                       'asset': quote_asset, 'change': total_quote})
    return changes


def _sync_open_position(trade: TradeData, all_positions: List[PositionData], all_balances: List[BalanceData]):
    """
    Синхронизирует открытые позиции, используя переданные (актуальные) данные о балансах.
    """
    existing_pos = _find_position(trade.symbol, trade.exchange, all_positions)
    base_asset = trade.symbol.split('/')[0]

    # Теперь эта функция получает УЖЕ обновленные балансы
    current_balance_obj = _find_balance(
        trade.exchange, base_asset, all_balances)

    # Этот баланс теперь актуален, так как он из обновленного в памяти списка
    final_net_amount = current_balance_obj.balance if current_balance_obj and current_balance_obj.balance is not None else Decimal(
        '0')

    zero_threshold = Decimal('1e-8')

    if final_net_amount <= zero_threshold:
        if existing_pos and existing_pos.row_number:
            sheets_service.delete_row(
                config.OPEN_POSITIONS_SHEET_NAME, existing_pos.row_number)
        return

    new_avg_price = trade.price
    if trade.trade_type == 'BUY':
        if existing_pos and existing_pos.net_amount > 0:
            # Расчет новой средней цены
            old_total_value = existing_pos.net_amount * existing_pos.avg_entry_price
            new_total_value = trade.amount * trade.price
            new_avg_price = (old_total_value +
                             new_total_value) / final_net_amount
    elif existing_pos:
        new_avg_price = existing_pos.avg_entry_price

    position_to_save = PositionData(
        row_number=existing_pos.row_number if existing_pos else None,
        symbol=trade.symbol, exchange=trade.exchange,
        net_amount=final_net_amount, avg_entry_price=new_avg_price,
        last_updated=datetime.now()
    )

    if existing_pos:
        sheets_service.update_position(position_to_save)
    else:
        sheets_service.add_position(position_to_save)


def log_fund_movement(
    movement_type: str, asset: str, amount: Decimal, timestamp: datetime, **kwargs: Any
) -> Tuple[bool, str]:
    movement_id = str(uuid.uuid4())
    logger.info(
        f"[LOGGER] Начало логирования движения средств. MoveID: {movement_id}")
    source_name = kwargs.get('source_name')
    destination_name = kwargs.get('destination_name')
    movement = MovementData(
        movement_id=movement_id, timestamp=timestamp, movement_type=movement_type.upper(),
        asset=asset.upper(), amount=amount,
        source_name=source_name.lower() if source_name else None,
        destination_name=destination_name.lower() if destination_name else None,
        fee_amount=kwargs.get('fee_amount'), fee_asset=kwargs.get('fee_asset'),
        notes=kwargs.get('notes'), transaction_id_blockchain=kwargs.get('transaction_id_blockchain')
    )
    if movement.movement_type in ['WITHDRAWAL', 'TRANSFER'] and movement.source_name:
        all_balances = sheets_service.get_all_balances()
        balance_obj = _find_balance(
            movement.source_name, movement.asset, all_balances)
        current_balance = balance_obj.balance if balance_obj and balance_obj.balance is not None else Decimal(
            '0')
        if current_balance < movement.amount:
            return False, f"Недостаточно {movement.asset} на счете {movement.source_name}."

    if not sheets_service.add_movement(movement):
        return False, "Ошибка записи движения средств."

    balance_changes = []
    if movement.source_name:
        balance_changes.append({'account': movement.source_name,
                               'asset': movement.asset, 'change': -movement.amount})
    if movement.destination_name:
        balance_changes.append({'account': movement.destination_name,
                               'asset': movement.asset, 'change': movement.amount})
    if balance_changes:
        if not sheets_service.batch_update_balances(balance_changes):
            return False, "Критическая ошибка: балансы не обновлены."

    logger.info(
        f"[LOGGER] Движение средств {movement_id} успешно залогировано.")
    return True, movement_id
