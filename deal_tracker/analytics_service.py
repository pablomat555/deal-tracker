# deal_tracker/analytics_service.py
import logging
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import datetime
from datetime import timezone, timedelta
import re

import sheets_service
import config

logger = logging.getLogger(__name__)

NUM_ANALYTICS_COLUMNS = getattr(config, 'NUM_ANALYTICS_COLUMNS_EXPECTED', 17)


def _safe_decimal_analytics(value, default_if_error=Decimal('0')) -> Decimal:
    if value is None:
        return default_if_error
    str_value = str(value).strip()
    if not str_value:
        return default_if_error
    str_value = str_value.replace(',', '.')
    try:
        return Decimal(str_value)
    except InvalidOperation:
        logger.debug(
            f"Ошибка конвертации '{value}' в Decimal в analytics, используется default: {default_if_error}")
        return default_if_error


def _calculate_pnl_metrics(fifo_logs: list[dict], open_positions: list[dict]) -> dict:
    """Рассчитывает метрики PNL."""
    logger.info("Расчет PNL метрик...")
    total_realized_pnl_fifo = sum(_safe_decimal_analytics(
        log.get('Fifo_PNL')) for log in fifo_logs)

    total_unrealized_pnl_open = Decimal('0')
    for pos in open_positions:
        unreal_pnl_val_str = pos.get('Unrealized_PNL')
        if unreal_pnl_val_str is not None and str(unreal_pnl_val_str).strip().lower() not in ['', 'n/a']:
            total_unrealized_pnl_open += _safe_decimal_analytics(
                unreal_pnl_val_str)

    net_total_pnl = total_realized_pnl_fifo + total_unrealized_pnl_open
    logger.info(
        f"PNL метрики: Realized={total_realized_pnl_fifo}, Unrealized={total_unrealized_pnl_open}, NetTotal={net_total_pnl}")
    return {
        'total_realized_pnl_fifo': total_realized_pnl_fifo,
        'total_unrealized_pnl_open': total_unrealized_pnl_open,
        'net_total_pnl': net_total_pnl
    }


def _calculate_trade_stats(fifo_logs: list[dict]) -> dict:
    """Рассчитывает статистику по закрытым сделкам."""
    logger.info("Расчет статистики по сделкам...")

    # ++ ИЗМЕНЕНИЕ: Все счетчики теперь Decimal для консистентности и исправления ошибки quantize ++
    total_trades_closed = Decimal(len(fifo_logs))
    winning_trades_closed, losing_trades_closed = Decimal('0'), Decimal('0')
    total_win_amount, total_loss_amount = Decimal('0'), Decimal('0')

    for log_entry in fifo_logs:
        pnl = _safe_decimal_analytics(log_entry.get('Fifo_PNL'))
        if pnl > Decimal('0'):
            winning_trades_closed += Decimal('1')
            total_win_amount += pnl
        elif pnl < Decimal('0'):
            losing_trades_closed += Decimal('1')
            total_loss_amount += abs(pnl)

    win_rate_percent = (winning_trades_closed / total_trades_closed *
                        100) if total_trades_closed > 0 else Decimal('0')
    average_win_amount = (
        total_win_amount / winning_trades_closed) if winning_trades_closed > 0 else Decimal('0')
    average_loss_amount = (
        total_loss_amount / losing_trades_closed) if losing_trades_closed > 0 else Decimal('0')

    profit_factor_str = "N/A"
    if total_win_amount > Decimal('0') and total_loss_amount == Decimal('0'):
        profit_factor_str = "Infinity"
    elif total_loss_amount > Decimal('0'):
        profit_factor_val = total_win_amount / total_loss_amount
        profit_factor_str = sheets_service.format_value_for_sheet(
            profit_factor_val.quantize(Decimal("0.01")))
    elif total_win_amount == Decimal('0') and total_loss_amount > Decimal('0'):
        profit_factor_str = sheets_service.format_value_for_sheet(
            Decimal('0').quantize(Decimal("0.01")))

    expectancy = ((win_rate_percent / 100 * average_win_amount) - ((losing_trades_closed / total_trades_closed if total_trades_closed >
                  0 else Decimal('0')) * average_loss_amount)) if total_trades_closed > 0 else Decimal('0')

    logger.info(
        f"Статистика по сделкам: TotalClosed={total_trades_closed}, WinRate={win_rate_percent:.2f}%")
    return {
        'total_trades_closed': total_trades_closed,
        'winning_trades_closed': winning_trades_closed,
        'losing_trades_closed': losing_trades_closed,
        'win_rate_percent': win_rate_percent,
        'average_win_amount': average_win_amount,
        'average_loss_amount': average_loss_amount,
        'profit_factor_str': profit_factor_str,
        'expectancy': expectancy
    }


def _calculate_financial_summary(fund_movements: list[dict], core_trades: list[dict]) -> dict:
    """Рассчитывает чистые вложения и суммарные комиссии."""
    logger.info("Расчет финансовых потоков (вложения, комиссии)...")
    investment_assets_list = getattr(config, 'INVESTMENT_ASSETS', [
                                     getattr(config, 'BASE_CURRENCY', 'USD').upper()])
    net_invested_funds = Decimal('0')
    for movement in fund_movements:
        move_type = str(movement.get('Type', '')).strip().upper()
        source_type = str(movement.get(
            'Source_Entity_Type', '')).strip().upper()
        dest_type = str(movement.get(
            'Destination_Entity_Type', '')).strip().upper()
        asset = str(movement.get('Asset', '')).strip().upper()
        amount_decimal = _safe_decimal_analytics(movement.get('Amount'))
        if amount_decimal > Decimal('0'):
            if asset in investment_assets_list:
                if move_type == 'DEPOSIT' and source_type == 'EXTERNAL':
                    net_invested_funds += amount_decimal
                elif move_type == 'WITHDRAWAL' and dest_type == 'EXTERNAL':
                    net_invested_funds -= amount_decimal
            elif (move_type == 'DEPOSIT' and source_type == 'EXTERNAL') or \
                 (move_type == 'WITHDRAWAL' and dest_type == 'EXTERNAL'):
                logger.info(
                    f"Net_Invested_Funds: Внешнее движение {asset} ({amount_decimal}) не учитывается.")

    total_commissions_paid_trades_base_ccy = Decimal('0')
    base_currency_for_comm = getattr(config, 'BASE_CURRENCY', 'USD').upper()
    for trade in core_trades:
        commission_amount_str = trade.get('Commission')
        commission_asset_str = str(
            trade.get('Commission_Asset', '')).strip().upper()
        if commission_amount_str is not None and str(commission_amount_str).strip():
            commission_val = _safe_decimal_analytics(commission_amount_str)
            if commission_val > Decimal('0'):
                if commission_asset_str == base_currency_for_comm:
                    total_commissions_paid_trades_base_ccy += commission_val
    total_commissions_paid_transfers_base_ccy = Decimal('0')
    for movement in fund_movements:
        fee_asset_str = str(movement.get('Fee_Asset', '')).strip().upper()
        if fee_asset_str == base_currency_for_comm:
            total_commissions_paid_transfers_base_ccy += _safe_decimal_analytics(
                movement.get('Fee_Amount'))
    total_commissions_paid_all_base_ccy = total_commissions_paid_trades_base_ccy + \
        total_commissions_paid_transfers_base_ccy

    logger.info(
        f"Финансовые потоки: NetInvested={net_invested_funds}, TotalCommissions={total_commissions_paid_all_base_ccy}")
    return {
        'net_invested_funds': net_invested_funds,
        'total_commissions_paid_all_base_ccy': total_commissions_paid_all_base_ccy
    }


def _calculate_portfolio_value_and_equity(open_positions: list[dict], net_invested_funds: Decimal, net_total_pnl: Decimal) -> dict:
    """Рассчитывает текущую стоимость портфеля и общий капитал."""
    logger.info("Расчет стоимости портфеля и общего капитала...")
    portfolio_current_value = Decimal('0')
    for pos in open_positions:
        net_amount = _safe_decimal_analytics(pos.get('Net_Amount'))
        current_price_str = pos.get('Current_Price')
        if net_amount > Decimal('0') and current_price_str and str(current_price_str).strip().lower() not in ['', 'n/a']:
            current_price = _safe_decimal_analytics(
                current_price_str, default_if_error=Decimal('NaN'))
            if not current_price.is_nan() and current_price > Decimal('0'):
                portfolio_current_value += net_amount * current_price
            else:
                logger.warning(
                    f"Для {pos.get('Symbol')} не удалось исп. Current_Price '{current_price_str}', стоимость не учтена.")
        elif net_amount > Decimal('0'):
            logger.warning(
                f"Для {pos.get('Symbol')} отсутствует Current_Price, стоимость не учтена.")

    total_equity = net_invested_funds + net_total_pnl
    logger.info(
        f"Портфель: CurrentValue={portfolio_current_value}, TotalEquity={total_equity}")
    return {
        'portfolio_current_value': portfolio_current_value,
        'total_equity': total_equity
    }


def process_fifo_transactions() -> tuple[bool, str]:
    logger.info("Запуск процесса обработки транзакций по FIFO...")
    try:
        all_trades_raw = sheets_service.get_all_core_trades()
        if all_trades_raw is None:
            logger.error(
                "FIFO: Не удалось получить сделки из Core_Trades. Обработка прервана.")
            return False, "Критическая ошибка: не удалось загрузить сделки для FIFO."
        logger.info(
            f"FIFO: Получено {len(all_trades_raw)} всего записей из Core_Trades.")
        if not all_trades_raw:
            return True, "Нет сделок для FIFO обработки."

        valid_trades = []
        for trade_dict_str_values in all_trades_raw:
            ts_str = trade_dict_str_values.get('Timestamp')
            trade_id = trade_dict_str_values.get('Trade_ID', 'N/A')
            row_number = trade_dict_str_values.get('row_number')
            if not ts_str or not isinstance(ts_str, str):
                logger.warning(
                    f"FIFO: Пропуск сделки {trade_id} (строка: {row_number}): Timestamp '{ts_str}' некорректен.")
                continue
            if row_number is None:
                logger.warning(
                    f"FIFO: Пропуск сделки {trade_id}: отсутствует 'row_number'.")
                continue
            try:
                ts_obj = datetime.datetime.strptime(
                    ts_str.strip(), "%Y-%m-%d %H:%M:%S")
                trade_copy = trade_dict_str_values.copy()
                trade_copy['Timestamp_dt'] = ts_obj
                trade_copy['Amount_dec'] = _safe_decimal_analytics(
                    trade_dict_str_values.get('Amount'))
                trade_copy['Price_dec'] = _safe_decimal_analytics(
                    trade_dict_str_values.get('Price'))
                trade_copy['Initial_Fifo_Consumed_Qty_dec'] = _safe_decimal_analytics(
                    trade_dict_str_values.get('Fifo_Consumed_Qty'))
                trade_copy['Current_Run_Consumed_Qty_dec'] = Decimal('0')
                valid_trades.append(trade_copy)
            except ValueError:
                logger.warning(
                    f"FIFO: Пропуск сделки {trade_id} (строка: {row_number}): неверный формат Timestamp '{ts_str}'.")
                continue

        buy_trades_all_symbols = sorted([t for t in valid_trades if str(
            t.get('Type', '')).strip().upper() == 'BUY'], key=lambda x: x['Timestamp_dt'])
        sell_trades_to_process = sorted([t for t in valid_trades if str(t.get('Type', '')).strip().upper() == 'SELL' and str(
            t.get('Fifo_Sell_Processed', 'FALSE')).strip().upper() != 'TRUE'], key=lambda x: x['Timestamp_dt'])

        if not sell_trades_to_process:
            return True, "Нет новых продаж для FIFO обработки."
        logger.info(
            f"FIFO: Найдено {len(buy_trades_all_symbols)} покупок и {len(sell_trades_to_process)} необработанных продаж.")

        fifo_log_entries_to_add = []
        core_trades_updates_payload = {}
        for sell_trade in sell_trades_to_process:
            sell_trade_id = sell_trade.get('Trade_ID')
            sell_symbol = sell_trade.get('Symbol')
            sell_qty_remaining = sell_trade['Amount_dec']
            sell_price = sell_trade['Price_dec']
            sell_timestamp_obj = sell_trade['Timestamp_dt']
            sell_row_number = sell_trade.get('row_number')
            temp_sell_processed_fully = False
            if sell_row_number is None:
                logger.error(
                    f"FIFO: Продажа ID {sell_trade_id} пропущена: отсутствует row_number.")
                continue
            for buy_trade in buy_trades_all_symbols:
                if sell_qty_remaining <= Decimal('0'):
                    break
                if buy_trade.get('Symbol') != sell_symbol:
                    continue
                buy_trade_id = buy_trade.get('Trade_ID')
                buy_row_number = buy_trade.get('row_number')
                if buy_row_number is None:
                    logger.error(
                        f"FIFO: Покупка ID {buy_trade_id} пропущена: отсутствует row_number.")
                    continue
                buy_total_qty = buy_trade['Amount_dec']
                buy_already_consumed_total = buy_trade['Initial_Fifo_Consumed_Qty_dec'] + \
                    buy_trade['Current_Run_Consumed_Qty_dec']
                buy_available_qty = buy_total_qty - buy_already_consumed_total
                buy_price = buy_trade['Price_dec']
                buy_timestamp_obj = buy_trade['Timestamp_dt']
                if buy_available_qty <= Decimal('0'):
                    continue
                matched_qty = min(sell_qty_remaining, buy_available_qty)
                min_meaningful_qty = Decimal(
                    config.QTY_PRECISION_STR_LOGGING) / Decimal('100')
                if matched_qty < min_meaningful_qty:
                    continue
                fifo_pnl = (sell_price - buy_price) * matched_qty

                fifo_log_entries_to_add.append([
                    sell_symbol,
                    buy_trade_id,
                    sell_trade_id,
                    matched_qty,
                    buy_price,
                    sell_price,
                    fifo_pnl,
                    sell_timestamp_obj.strftime("%Y-%m-%d %H:%M:%S"),
                    buy_timestamp_obj.strftime("%Y-%m-%d %H:%M:%S"),
                    sell_trade.get('Exchange')
                ])

                buy_trade['Current_Run_Consumed_Qty_dec'] += matched_qty
                final_consumed_for_buy = buy_trade['Initial_Fifo_Consumed_Qty_dec'] + \
                    buy_trade['Current_Run_Consumed_Qty_dec']
                if buy_row_number not in core_trades_updates_payload:
                    core_trades_updates_payload[buy_row_number] = {}
                core_trades_updates_payload[buy_row_number]['Fifo_Consumed_Qty'] = final_consumed_for_buy
                sell_qty_remaining -= matched_qty
                if sell_qty_remaining < min_meaningful_qty:
                    sell_qty_remaining = Decimal('0')
                    temp_sell_processed_fully = True
                    break
            if temp_sell_processed_fully:
                if sell_row_number not in core_trades_updates_payload:
                    core_trades_updates_payload[sell_row_number] = {}
                core_trades_updates_payload[sell_row_number]['Fifo_Sell_Processed'] = "TRUE"
            elif sell_qty_remaining > Decimal('0'):
                logger.warning(
                    f"FIFO: Для продажи ID {sell_trade_id} (строка: {sell_row_number}) не хватило покупок. Остаток: {sell_qty_remaining}.")

        final_updates_for_sheets = [{'row_number': rn, **upd}
                                    for rn, upd in core_trades_updates_payload.items()]

        if fifo_log_entries_to_add:
            if not sheets_service.batch_append_to_fifo_log(fifo_log_entries_to_add):
                logger.error(
                    f"FIFO: Ошибка записи {len(fifo_log_entries_to_add)} записей в Fifo_Log.")
                return False, "Ошибка записи в Fifo_Log."
        if final_updates_for_sheets:
            if not sheets_service.batch_update_core_trades_fifo_fields(final_updates_for_sheets):
                logger.error(
                    f"FIFO: Ошибка обновления {len(final_updates_for_sheets)} строк в Core_Trades.")
                return False, "Ошибка обновления Core_Trades."

        msg = f"FIFO обработка завершена. Добавлено {len(fifo_log_entries_to_add)} записей в Fifo_Log. Обновлено {len(final_updates_for_sheets)} строк в Core_Trades."
        logger.info(msg)
        return True, msg
    except Exception as e:
        logger.error(
            f"Критическая ошибка в FIFO обработке: {e}", exc_info=True)
        return False, f"Критическая ошибка FIFO: {e}"


def calculate_and_update_analytics_sheet(triggered_by_context: str = "scheduled_or_unknown") -> tuple[bool, str]:
    logger.info(
        f"Запуск расчета и обновления листа Analytics... (Источник: {triggered_by_context})")
    date_generated = (datetime.datetime.now(
        timezone.utc) + timedelta(hours=config.TZ_OFFSET_HOURS)).strftime("%Y-%m-%d %H:%M:%S")

    fifo_success, fifo_message = process_fifo_transactions()
    if not fifo_success:
        logger.error(
            f"Analytics (triggered: {triggered_by_context}, date_gen: {date_generated}): Ошибка обработки FIFO, обновление Analytics прервано: {fifo_message}")
        return False, f"Ошибка обработки FIFO: {fifo_message}"
    logger.info(
        f"Analytics (triggered: {triggered_by_context}, date_gen: {date_generated}): Сообщение от FIFO-обработки: {fifo_message}")

    try:
        all_fifo_logs = sheets_service.get_all_fifo_logs()
        all_core_trades_str = sheets_service.get_all_core_trades()
        all_fund_movements_str = sheets_service.get_all_fund_movements()
        all_open_positions_str = sheets_service.get_all_open_positions()

        if any(data is None for data in [all_fifo_logs, all_core_trades_str, all_fund_movements_str, all_open_positions_str]):
            missing_data_sources = []
            if all_fifo_logs is None:
                missing_data_sources.append("Fifo_Log")
            if all_core_trades_str is None:
                missing_data_sources.append("Core_Trades")
            if all_fund_movements_str is None:
                missing_data_sources.append("Fund_Movements")
            if all_open_positions_str is None:
                missing_data_sources.append("Open_Positions")
            error_msg = f"Не удалось загрузить данные из: {', '.join(missing_data_sources)}. Обновление аналитики прервано."
            logger.error(
                f"Analytics (triggered: {triggered_by_context}, date_gen: {date_generated}): {error_msg}")
            return False, error_msg

        logger.info(f"Analytics (triggered: {triggered_by_context}, date_gen: {date_generated}): Получено {len(all_fifo_logs)} из Fifo_Log, {len(all_core_trades_str)} из Core_Trades, {len(all_fund_movements_str)} из Fund_Movements, {len(all_open_positions_str)} из Open_Positions.")
        if len(all_open_positions_str) > 0:
            logger.debug(
                f"Analytics: Первая запись из Open_Positions: {all_open_positions_str[0]}")

        pnl_metrics = _calculate_pnl_metrics(
            all_fifo_logs, all_open_positions_str)
        trade_stats = _calculate_trade_stats(all_fifo_logs)
        financial_summary = _calculate_financial_summary(
            all_fund_movements_str, all_core_trades_str)
        portfolio_metrics = _calculate_portfolio_value_and_equity(
            all_open_positions_str, financial_summary['net_invested_funds'], pnl_metrics['net_total_pnl'])

        analytics_row = [
            date_generated,
            sheets_service.format_value_for_sheet(
                pnl_metrics['total_realized_pnl_fifo'].quantize(Decimal(config.USD_DISPLAY_PRECISION))),
            sheets_service.format_value_for_sheet(
                pnl_metrics['total_unrealized_pnl_open'].quantize(Decimal(config.USD_DISPLAY_PRECISION))),
            sheets_service.format_value_for_sheet(
                pnl_metrics['net_total_pnl'].quantize(Decimal(config.USD_DISPLAY_PRECISION))),
            # ++ ИЗМЕНЕНИЕ: передаем Decimal как есть, форматирование внутри sheets_service ++
            sheets_service.format_value_for_sheet(
                trade_stats['total_trades_closed']),
            sheets_service.format_value_for_sheet(
                trade_stats['winning_trades_closed']),
            sheets_service.format_value_for_sheet(
                trade_stats['losing_trades_closed']),
            sheets_service.format_value_for_sheet(
                trade_stats['win_rate_percent'].quantize(Decimal("0.01"))),
            sheets_service.format_value_for_sheet(
                trade_stats['average_win_amount'].quantize(Decimal(config.USD_DISPLAY_PRECISION))),
            sheets_service.format_value_for_sheet(
                trade_stats['average_loss_amount'].quantize(Decimal(config.USD_DISPLAY_PRECISION))),
            trade_stats['profit_factor_str'],
            sheets_service.format_value_for_sheet(
                trade_stats['expectancy'].quantize(Decimal(config.USD_DISPLAY_PRECISION))),
            sheets_service.format_value_for_sheet(
                financial_summary['total_commissions_paid_all_base_ccy'].quantize(Decimal(config.USD_DISPLAY_PRECISION))),
            sheets_service.format_value_for_sheet(
                financial_summary['net_invested_funds'].quantize(Decimal(config.USD_DISPLAY_PRECISION))),
            sheets_service.format_value_for_sheet(
                portfolio_metrics['portfolio_current_value'].quantize(Decimal(config.USD_DISPLAY_PRECISION))),
            sheets_service.format_value_for_sheet(
                portfolio_metrics['total_equity'].quantize(Decimal(config.USD_DISPLAY_PRECISION))),
            ""
        ]

        while len(analytics_row) < NUM_ANALYTICS_COLUMNS:
            analytics_row.append("")
        if len(analytics_row) > NUM_ANALYTICS_COLUMNS:
            analytics_row = analytics_row[:NUM_ANALYTICS_COLUMNS]

        if sheets_service.append_to_sheet(config.ANALYTICS_SHEET_NAME, analytics_row):
            success_msg = f"Лист Analytics успешно обновлен данными от {date_generated}."
            logger.info(
                f"Analytics (triggered: {triggered_by_context}): {success_msg}")
            return True, success_msg
        else:
            error_msg = "Ошибка записи данных в лист Analytics."
            logger.error(
                f"Analytics (triggered: {triggered_by_context}, date_gen: {date_generated}): {error_msg}. Строка: {str(analytics_row)[:200]}...")
            return False, error_msg

    except Exception as e:
        error_msg = f"Критическая ошибка при обновлении Analytics: {e}"
        logger.error(
            f"Analytics (triggered: {triggered_by_context}, date_gen: {date_generated}): {error_msg}", exc_info=True)
        return False, error_msg


if __name__ == '__main__':
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("Переменные .env загружены (если есть).")
    except:
        pass
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(
            level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.info("Запуск тестового обновления Analytics (из __main__)...")
    success, message = calculate_and_update_analytics_sheet(
        triggered_by_context="__main__ test run")
    print(f"Результат: Success={success}, Message='{message}'")
