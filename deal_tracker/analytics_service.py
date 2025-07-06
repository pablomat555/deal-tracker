import logging
import time
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Dict, Any

import sheets_service
import config
from models import TradeData, FifoLogData, PositionData, MovementData, AnalyticsData

# --- Настройка логирования ---
logging.basicConfig(level=config.LOG_LEVEL, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Вспомогательные функции ---
def _safe_decimal(val: Any) -> Decimal:
    """Безопасно преобразует значение в Decimal, обрабатывая None и некорректные строки."""
    if val is None: return Decimal(0)
    try:
        return Decimal(val)
    except (InvalidOperation, TypeError):
        try:
            # Попытка очистить строку от лишних символов и преобразовать
            cleaned_val = ''.join(c for c in str(val) if c in '0123456789.,-').replace(',', '.')
            return Decimal(cleaned_val)
        except (InvalidOperation, TypeError):
            return Decimal(0)

def _calculate_pnl_metrics(fifo_logs: List[FifoLogData], open_positions: List[PositionData]) -> Tuple[Decimal, Decimal, Decimal]:
    """Рассчитывает реализованный, нереализованный и общий PNL."""
    total_realized = sum((log.fifo_pnl for log in fifo_logs if log.fifo_pnl), Decimal('0'))
    total_unrealized = sum((_safe_decimal(pos.unrealized_pnl) for pos in open_positions if pos.unrealized_pnl is not None), Decimal('0'))
    return total_realized, total_unrealized, total_realized + total_unrealized

def _calculate_trade_stats(fifo_logs: List[FifoLogData]) -> Dict[str, Any]:
    """Рассчитывает статистику по закрытым сделкам (винрейт, профит-фактор и т.д.)."""
    winning_trades = [log for log in fifo_logs if log.fifo_pnl and log.fifo_pnl > 0]
    losing_trades = [log for log in fifo_logs if log.fifo_pnl and log.fifo_pnl < 0]
    total_closed = len(fifo_logs)
    if total_closed == 0: return {'total_trades_closed': 0, 'win_rate_percent': Decimal(0)}
    
    total_win_amount = sum(t.fifo_pnl for t in winning_trades)
    total_loss_amount = sum(t.fifo_pnl for t in losing_trades)
    avg_win = total_win_amount / len(winning_trades) if winning_trades else Decimal(0)
    avg_loss = total_loss_amount / len(losing_trades) if losing_trades else Decimal(0)
    
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
        'profit_factor': profit_factor
    }

def process_fifo_transactions(all_trades: List[TradeData]) -> Tuple[bool, str, List[FifoLogData]]:
    """
    Обрабатывает транзакции по методу FIFO, используя уже загруженные данные.
    Возвращает статус, сообщение и список созданных логов FIFO.
    """
    logger.info("Запуск FIFO обработки...")
    if not all_trades: return True, "Нет сделок для FIFO обработки.", []

    all_trades.sort(key=lambda t: t.timestamp)
    buys = [t for t in all_trades if t.trade_type == 'BUY']
    sells_to_process = [t for t in all_trades if t.trade_type == 'SELL' and not t.fifo_sell_processed]

    if not sells_to_process: return True, "Нет новых продаж для FIFO обработки.", []

    fifo_log_entries, trade_updates, consumed_buy_quantities = [], [], {}
    for sell in sells_to_process:
        sell_qty_remaining = sell.amount
        for buy in buys:
            if sell_qty_remaining <= 0: break
            if buy.symbol != sell.symbol: continue
            
            initial_consumed = consumed_buy_quantities.get(buy.trade_id, buy.fifo_consumed_qty or Decimal(0))
            available_buy_qty = buy.amount - initial_consumed
            if available_buy_qty <= 0: continue
            
            matched_qty = min(sell_qty_remaining, available_buy_qty)
            pnl = (sell.price - buy.price) * matched_qty
            
            fifo_log_entries.append(FifoLogData(
                symbol=sell.symbol, buy_trade_id=buy.trade_id, sell_trade_id=sell.trade_id,
                matched_qty=matched_qty, buy_price=buy.price, sell_price=sell.price, fifo_pnl=pnl,
                timestamp_closed=sell.timestamp, buy_timestamp=buy.timestamp, exchange=sell.exchange
            ))
            consumed_buy_quantities[buy.trade_id] = initial_consumed + matched_qty
            sell_qty_remaining -= matched_qty
            
        trade_updates.append({'row_number': sell.row_number, 'fifo_sell_processed': True})

    for trade_id, consumed_qty in consumed_buy_quantities.items():
        trade_to_update = next((t for t in buys if t.trade_id == trade_id), None)
        if trade_to_update: trade_updates.append({'row_number': trade_to_update.row_number, 'fifo_consumed_qty': consumed_qty})

    logs_ok = sheets_service.batch_append_fifo_logs(fifo_log_entries)
    updates_ok = sheets_service.batch_update_trades_fifo_fields(trade_updates)
    if not logs_ok or not updates_ok: return False, "Ошибка при записи результатов FIFO.", []
    
    msg = f"FIFO: обработано {len(sells_to_process)} продаж, создано {len(fifo_log_entries)} логов."
    logger.info(msg)
    return True, msg, fifo_log_entries

def calculate_and_update_analytics_sheet() -> Tuple[bool, str]:
    """Главная функция, запускающая полный пересчет с оптимизированной загрузкой данных."""
    logger.info("Запуск полного обновления аналитики...")

    # Шаг 1: Загружаем все необходимые данные ОДНИМ пакетным запросом
    sheets_to_fetch = {
        config.CORE_TRADES_SHEET_NAME: TradeData,
        config.OPEN_POSITIONS_SHEET_NAME: PositionData,
        config.FUND_MOVEMENTS_SHEET_NAME: MovementData
    }
    all_data, errors = sheets_service.batch_get_records(sheets_to_fetch)
    if errors:
        return False, f"Критическая ошибка чтения данных из Sheets: {errors}"

    core_trades = all_data.get(config.CORE_TRADES_SHEET_NAME, [])
    open_positions = all_data.get(config.OPEN_POSITIONS_SHEET_NAME, [])
    fund_movements = all_data.get(config.FUND_MOVEMENTS_SHEET_NAME, [])

    # Шаг 2: Обрабатываем FIFO, передавая уже загруженные сделки
    fifo_success, fifo_message, new_fifo_logs = process_fifo_transactions(core_trades)
    if not fifo_success:
        return False, fifo_message

    # Шаг 3: Загружаем все логи FIFO.
    fifo_logs, _ = sheets_service.get_all_records(config.FIFO_LOG_SHEET_NAME, FifoLogData)
    
    # Шаг 4: Расчеты
    realized_pnl, unrealized_pnl, net_pnl = _calculate_pnl_metrics(fifo_logs, open_positions)
    stats = _calculate_trade_stats(fifo_logs)
    total_commissions = sum(_safe_decimal(t.commission) for t in core_trades if t.commission)
    net_invested = sum(_safe_decimal(m.amount) for m in fund_movements if m.movement_type == 'DEPOSIT') - \
        sum(_safe_decimal(m.amount) for m in fund_movements if m.movement_type == 'WITHDRAWAL')
    current_portfolio_value = sum(_safe_decimal(pos.net_amount) * _safe_decimal(pos.current_price) for pos in open_positions if pos.net_amount and pos.current_price)
    
    # Шаг 5: Формируем и записываем отчет с учетом часового пояса
    target_timezone = timezone(timedelta(hours=config.TZ_OFFSET_HOURS))
    analytics_record = AnalyticsData(
        date_generated=datetime.now(timezone.utc).astimezone(target_timezone),
        total_realized_pnl=realized_pnl, total_unrealized_pnl=unrealized_pnl, net_total_pnl=net_pnl,
        total_trades_closed=stats.get('total_trades_closed', 0), winning_trades_closed=stats.get('winning_trades_closed', 0),
        losing_trades_closed=stats.get('losing_trades_closed', 0), win_rate_percent=stats.get('win_rate_percent', Decimal(0)),
        average_win_amount=stats.get('average_win_amount', Decimal(0)), average_loss_amount=stats.get('average_loss_amount', Decimal(0)),
        profit_factor=stats.get('profit_factor', "N/A"), expectancy=Decimal(0), total_commissions_paid=total_commissions,
        net_invested_funds=net_invested, portfolio_current_value=current_portfolio_value, total_equity=net_invested + net_pnl,
    )

    if sheets_service.update_or_create_analytics_record(analytics_record):
        msg = f"Аналитика успешно обновлена. {fifo_message}"; logger.info(msg); return True, msg
    else:
        return False, "Не удалось записать итоговую строку аналитики."

# --- Главный блок запуска ---
if __name__ == "__main__":
    logger.info("Сервис аналитики запущен в циклическом режиме.")
    while True:
        try:
            success, message = calculate_and_update_analytics_sheet()
            sheets_service.update_system_status("OK" if success else "ERROR", datetime.now())
            if success:
                logger.info(f"Цикл обновления аналитики завершен. {message}")
            else:
                logger.error(f"Цикл обновления аналитики завершился с ошибкой: {message}")
        except Exception as e:
            logger.critical(f"Критическая ошибка в главном цикле сервиса аналитики: {e}", exc_info=True)
            sheets_service.update_system_status("CRITICAL_ERROR", datetime.now())
        
        logger.info(f"Следующий запуск через {config.PRICE_UPDATE_INTERVAL_SECONDS} секунд.")
        time.sleep(config.PRICE_UPDATE_INTERVAL_SECONDS)
