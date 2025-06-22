# deal_tracker/analytics_service.py
import logging
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import List, Tuple

import sheets_service
import config
from models import TradeData, FifoLogData, PositionData, MovementData, AnalyticsData

logger = logging.getLogger(__name__)


# ИСПРАВЛЕНО: Добавлена "безопасная" функция для преобразования в Decimal
def _safe_decimal(val):
    """Безопасно преобразует значение в Decimal, возвращая 0 при ошибке."""
    if val is None:
        return Decimal(0)
    try:
        # Пробуем прямое преобразование, если это уже число или корректная строка
        return Decimal(val)
    except (InvalidOperation, TypeError, ValueError):
        # Если не удалось, пробуем очистить строку (например, от валютных символов)
        try:
            # Оставляем только цифры, точку, запятую и знак минуса
            cleaned_val = ''.join(c for c in str(val) if c in '0123456789.,-')
            cleaned_val = cleaned_val.replace(',', '.')
            return Decimal(cleaned_val)
        except (InvalidOperation, TypeError, ValueError):
            return Decimal(0)


def _calculate_pnl_metrics(fifo_logs: List[FifoLogData], open_positions: List[PositionData]) -> Tuple[Decimal, Decimal, Decimal]:
    """Рассчитывает PNL, работая с типизированными моделями."""
    total_realized = sum(
        (log.fifo_pnl for log in fifo_logs if log.fifo_pnl), Decimal('0'))

    # ИСПРАВЛЕНО: Используется _safe_decimal для обработки "грязных" данных
    total_unrealized = sum(
        pos.unrealized_pnl if isinstance(pos.unrealized_pnl, Decimal)
        else _safe_decimal(pos.unrealized_pnl)
        for pos in open_positions if pos.unrealized_pnl is not None
    )

    net_total = total_realized + total_unrealized
    return total_realized, total_unrealized, net_total


def _calculate_trade_stats(fifo_logs: List[FifoLogData]) -> dict:
    """Рассчитывает статистику по закрытым сделкам."""
    winning_trades = [
        log for log in fifo_logs if log.fifo_pnl and log.fifo_pnl > 0]
    losing_trades = [
        log for log in fifo_logs if log.fifo_pnl and log.fifo_pnl < 0]

    total_closed = len(fifo_logs)
    if total_closed == 0:
        return {'total_trades_closed': 0, 'win_rate_percent': Decimal(0)}

    total_win_amount = sum(t.fifo_pnl for t in winning_trades)
    total_loss_amount = sum(t.fifo_pnl for t in losing_trades)

    avg_win = total_win_amount / \
        len(winning_trades) if winning_trades else Decimal(0)
    avg_loss = total_loss_amount / \
        len(losing_trades) if losing_trades else Decimal(0)

    profit_factor = "Infinity"
    if total_loss_amount != 0:
        profit_factor = f"{-total_win_amount / total_loss_amount:.2f}"

    return {
        'total_trades_closed': total_closed,
        'winning_trades_closed': len(winning_trades),
        'losing_trades_closed': len(losing_trades),
        'win_rate_percent': (Decimal(len(winning_trades)) / total_closed) * 100,
        'average_win_amount': avg_win,
        'average_loss_amount': avg_loss,
        'profit_factor': profit_factor,
    }


def process_fifo_transactions() -> Tuple[bool, str]:
    """Обрабатывает транзакции по FIFO, работая с моделями TradeData."""
    logger.info("Запуск FIFO обработки...")
    all_trades = sheets_service.get_all_core_trades()
    if not all_trades:
        return True, "Нет сделок для FIFO обработки."

    # Сортируем сделки по дате
    all_trades.sort(key=lambda t: t.timestamp)

    buys = [t for t in all_trades if t.trade_type == 'BUY']
    sells_to_process = [t for t in all_trades if t.trade_type ==
                        'SELL' and not t.fifo_sell_processed]

    if not sells_to_process:
        return True, "Нет новых продаж для FIFO обработки."

    fifo_log_entries = []
    trade_updates = []

    # {trade_id: consumed_qty}
    consumed_buy_quantities: dict[str, Decimal] = {}

    for sell in sells_to_process:
        sell_qty_remaining = sell.amount

        for buy in buys:
            if sell_qty_remaining <= 0:
                break
            if buy.symbol != sell.symbol:
                continue

            # Сколько от этой покупки уже было использовано в предыдущих итерациях
            initial_consumed = consumed_buy_quantities.get(
                buy.trade_id, buy.fifo_consumed_qty or Decimal(0))
            available_buy_qty = buy.amount - initial_consumed

            if available_buy_qty <= 0:
                continue

            matched_qty = min(sell_qty_remaining, available_buy_qty)

            pnl = (sell.price - buy.price) * matched_qty

            # Создаем запись для лога FIFO
            fifo_log_entries.append(FifoLogData(
                symbol=sell.symbol, buy_trade_id=buy.trade_id, sell_trade_id=sell.trade_id,
                matched_qty=matched_qty, buy_price=buy.price, sell_price=sell.price, fifo_pnl=pnl,
                timestamp_closed=sell.timestamp, buy_timestamp=buy.timestamp, exchange=sell.exchange
            ))

            # Обновляем инфо о потребленном количестве
            consumed_buy_quantities[buy.trade_id] = initial_consumed + matched_qty
            sell_qty_remaining -= matched_qty

        # Помечаем продажу как обработанную
        trade_updates.append(
            {'row_number': sell.row_number, 'fifo_sell_processed': True})

    # Добавляем обновления для покупок
    for trade_id, consumed_qty in consumed_buy_quantities.items():
        # Находим нужный трейд, чтобы получить его row_number
        trade_to_update = next(
            (t for t in buys if t.trade_id == trade_id), None)
        if trade_to_update:
            trade_updates.append(
                {'row_number': trade_to_update.row_number, 'fifo_consumed_qty': consumed_qty})

    # Пакетно записываем все изменения
    logs_ok = sheets_service.batch_append_fifo_logs(fifo_log_entries)
    updates_ok = sheets_service.batch_update_trades_fifo_fields(trade_updates)

    if not logs_ok or not updates_ok:
        return False, "Ошибка при записи результатов FIFO в Google Sheets."

    msg = f"FIFO: обработано {len(sells_to_process)} продаж, создано {len(fifo_log_entries)} логов."
    logger.info(msg)
    return True, msg


def calculate_and_update_analytics_sheet() -> Tuple[bool, str]:
    """Главная функция, запускающая полный пересчет и обновление листа аналитики."""
    logger.info("Запуск полного обновления аналитики...")

    fifo_success, fifo_message = process_fifo_transactions()
    if not fifo_success:
        return False, fifo_message

    # Загружаем все данные заново, т.к. FIFO мог их изменить
    fifo_logs = sheets_service.get_all_fifo_logs()
    open_positions = sheets_service.get_all_open_positions()
    # Другие данные (движения, все сделки) можно было бы не перезагружать, но для надежности сделаем
    fund_movements = sheets_service.get_all_fund_movements()
    all_trades = sheets_service.get_all_core_trades()  # Нужны для подсчета комиссий

    # Расчеты
    realized_pnl, unrealized_pnl, net_pnl = _calculate_pnl_metrics(
        fifo_logs, open_positions)
    stats = _calculate_trade_stats(fifo_logs)

    total_commissions = sum(_safe_decimal(t.commission)
                            for t in all_trades if t.commission)

    net_invested = sum(_safe_decimal(m.amount) for m in fund_movements if m.movement_type == 'DEPOSIT') - \
        sum(_safe_decimal(m.amount)
            for m in fund_movements if m.movement_type == 'WITHDRAWAL')

    current_portfolio_value = sum(_safe_decimal(pos.net_amount) * _safe_decimal(pos.current_price)
                                  for pos in open_positions if pos.net_amount and pos.current_price)

    # Формируем объект данных для записи
    analytics_record = AnalyticsData(
        date_generated=datetime.now(),
        total_realized_pnl=realized_pnl,
        total_unrealized_pnl=unrealized_pnl,
        net_total_pnl=net_pnl,
        total_trades_closed=stats.get('total_trades_closed', 0),
        winning_trades_closed=stats.get('winning_trades_closed', 0),
        losing_trades_closed=stats.get('losing_trades_closed', 0),
        win_rate_percent=stats.get('win_rate_percent', Decimal(0)),
        average_win_amount=stats.get('average_win_amount', Decimal(0)),
        average_loss_amount=stats.get('average_loss_amount', Decimal(0)),
        profit_factor=stats.get('profit_factor', "N/A"),
        expectancy=Decimal(0),  # Заглушка
        total_commissions_paid=total_commissions,
        net_invested_funds=net_invested,
        portfolio_current_value=current_portfolio_value,
        total_equity=net_invested + net_pnl,
    )

    # Запись в таблицу
    if sheets_service.add_analytics_record(analytics_record):
        msg = f"Аналитика успешно обновлена. {fifo_message}"
        logger.info(msg)
        return True, msg
    else:
        return False, "Не удалось записать итоговую строку аналитики."
