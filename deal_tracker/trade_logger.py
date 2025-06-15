# deal_tracker/trade_logger.py
import uuid
from datetime import datetime, timezone, timedelta
import logging
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation

import sheets_service
import config

logger = logging.getLogger(__name__)

USD_QUANTIZER_LOGGING = Decimal(config.USD_PRECISION_STR_LOGGING)
QTY_QUANTIZER_LOGGING = Decimal(config.QTY_PRECISION_STR_LOGGING)
PRICE_QUANTIZER_LOGGING = Decimal(config.PRICE_PRECISION_STR_LOGGING)

USD_QUANTIZER_DISPLAY = Decimal(config.USD_DISPLAY_PRECISION)
QTY_QUANTIZER_DISPLAY = Decimal(config.QTY_DISPLAY_PRECISION)


def _safe_decimal(value, quantizer: Decimal, default_if_none=Decimal('0')) -> Decimal | None:
    if value is None:
        return None if default_if_none is None else default_if_none
    str_value = str(value).replace(',', '.').strip()
    if not str_value:
        return None if default_if_none is None else default_if_none
    try:
        return Decimal(str_value).quantize(quantizer, rounding=ROUND_HALF_UP)
    except InvalidOperation:
        logger.warning(
            f"Не удалось конвертировать '{value}' в Decimal. Default: {default_if_none}")
        return None if default_if_none is None else default_if_none


def _parse_symbol(symbol: str) -> tuple[str | None, str | None]:
    parts = symbol.upper().split('/')
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    logger.error(
        f"Не удалось распарсить символ '{symbol}'. Ожидался 'BASE/QUOTE'.")
    return None, None


def _format_for_user_message(value: Decimal | None, asset_name: str | None = None) -> str:
    if value is None:
        return "N/A"
    quantizer = QTY_QUANTIZER_DISPLAY
    if asset_name:
        asset_upper = asset_name.upper()
        stable_or_base_currencies = [
            config.BASE_CURRENCY.upper(), 'USD', 'USDT', 'USDC', 'DAI', 'BUSD']
        if asset_upper in stable_or_base_currencies:
            quantizer = USD_QUANTIZER_DISPLAY
    return str(value.quantize(quantizer, rounding=ROUND_HALF_UP))


def log_trade(
    trade_type: str, symbol: str, qty_str: str, price_str: str, source: str,
    exchange_position_name: str | None = None, strategy_position_name: str | None = None,
    optional_fields: dict | None = None, order_id: str | None = None,
    asset_type: str | None = "SPOT"
) -> tuple[bool, str | None]:
    generated_trade_id = str(uuid.uuid4())
    target_timezone = timezone(timedelta(hours=config.TZ_OFFSET_HOURS))
    final_timestamp_obj = datetime.now(
        timezone.utc).astimezone(target_timezone)
    if optional_fields is None:
        optional_fields = {}

    date_input_str = optional_fields.get('date')
    if date_input_str:
        try:
            date_input_str = date_input_str.strip()
            dt_obj = None
            if len(date_input_str) == 19:
                dt_obj = datetime.strptime(date_input_str, "%Y-%m-%d %H:%M:%S")
            elif len(date_input_str) == 16:
                dt_obj = datetime.strptime(date_input_str, "%Y-%m-%d %H:%M")
            elif len(date_input_str) == 10:
                dt_obj = datetime.strptime(
                    date_input_str, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
            if dt_obj:
                final_timestamp_obj = dt_obj.replace(tzinfo=target_timezone)
        except ValueError:
            logger.warning(
                f"Ошибка парсинга даты '{date_input_str}'. Исп. текущее время.")
    timestamp_str = final_timestamp_obj.strftime("%Y-%m-%d %H:%M:%S")

    try:
        qty = _safe_decimal(qty_str, QTY_QUANTIZER_LOGGING)
        price = _safe_decimal(price_str, PRICE_QUANTIZER_LOGGING)
        base_asset, quote_asset = _parse_symbol(symbol)

        if not all([qty, price, base_asset, quote_asset]) or qty <= Decimal('0') or price <= Decimal('0'):
            return False, "Количество, цена или символ указаны некорректно."

        # ИСПРАВЛЕНО: Приводим имя биржи к нижнему регистру в самом начале
        final_exchange_name = (
            exchange_position_name or optional_fields.get('exch', '')).strip().lower()
        if not final_exchange_name:
            return False, "Не указана биржа (exch:ИМЯ_БИРЖИ)."

        initial_balances_map = sheets_service.get_all_balances()
        if initial_balances_map is None:
            logger.error(
                "Критическая ошибка: initial_balances_map is None в log_trade.")
            return False, "Внутренняя ошибка: не удалось загрузить балансы."

        total_quote_amount = (
            qty * price).quantize(USD_QUANTIZER_LOGGING, rounding=ROUND_HALF_UP)
        fee_amt_str = optional_fields.get('fee', optional_fields.get('com'))
        fee_asset_str = optional_fields.get('fee_asset')
        fee_amount = Decimal('0')
        fee_asset_parsed = None
        if fee_amt_str:
            fee_asset_candidate = (fee_asset_str or quote_asset).upper()
            fee_quantizer = USD_QUANTIZER_LOGGING if fee_asset_candidate == config.BASE_CURRENCY else QTY_QUANTIZER_LOGGING
            temp_fee = _safe_decimal(fee_amt_str, fee_quantizer, Decimal('0'))
            if temp_fee and temp_fee > Decimal('0'):
                fee_amount, fee_asset_parsed = temp_fee, fee_asset_candidate
                logger.info(
                    f"Сделка: Обнаружена комиссия {fee_amount} {fee_asset_parsed}")

        updates_for_balances = []
        trade_type_upper = trade_type.upper()

        if trade_type_upper == 'BUY':
            cost_main = total_quote_amount
            if fee_asset_parsed == quote_asset:
                cost_main += fee_amount
            if not sheets_service.has_sufficient_balance(final_exchange_name, quote_asset, cost_main, initial_balances_map):
                cur_bal = sheets_service.get_account_balance(
                    final_exchange_name, quote_asset, initial_balances_map)
                return False, f"Недостаточно {quote_asset} на {final_exchange_name}. Нужно: {_format_for_user_message(cost_main, quote_asset)}, доступно: {_format_for_user_message(cur_bal, quote_asset)}."
            updates_for_balances.append(
                {'account': final_exchange_name, 'asset': quote_asset, 'change': -cost_main})
            if fee_amount > Decimal('0') and fee_asset_parsed != quote_asset:
                if not sheets_service.has_sufficient_balance(final_exchange_name, fee_asset_parsed, fee_amount, initial_balances_map):
                    cur_bal = sheets_service.get_account_balance(
                        final_exchange_name, fee_asset_parsed, initial_balances_map)
                    return False, f"Недостаточно {fee_asset_parsed} на {final_exchange_name} для комиссии. Нужно: {_format_for_user_message(fee_amount, fee_asset_parsed)}, доступно: {_format_for_user_message(cur_bal, fee_asset_parsed)}."
                updates_for_balances.append(
                    {'account': final_exchange_name, 'asset': fee_asset_parsed, 'change': -fee_amount})
            updates_for_balances.append(
                {'account': final_exchange_name, 'asset': base_asset, 'change': qty})
        elif trade_type_upper == 'SELL':
            cost_main = qty
            if fee_asset_parsed == base_asset:
                cost_main += fee_amount
            if not sheets_service.has_sufficient_balance(final_exchange_name, base_asset, cost_main, initial_balances_map):
                cur_bal = sheets_service.get_account_balance(
                    final_exchange_name, base_asset, initial_balances_map)
                return False, f"Недостаточно {base_asset} на {final_exchange_name}. Нужно: {_format_for_user_message(cost_main, base_asset)}, доступно: {_format_for_user_message(cur_bal, base_asset)}."
            updates_for_balances.append(
                {'account': final_exchange_name, 'asset': base_asset, 'change': -cost_main})
            proceeds_quote = total_quote_amount
            if fee_asset_parsed == quote_asset:
                proceeds_quote -= fee_amount
            updates_for_balances.append(
                {'account': final_exchange_name, 'asset': quote_asset, 'change': proceeds_quote})
            if fee_amount > Decimal('0') and fee_asset_parsed and fee_asset_parsed != base_asset and fee_asset_parsed != quote_asset:
                if not sheets_service.has_sufficient_balance(final_exchange_name, fee_asset_parsed, fee_amount, initial_balances_map):
                    cur_bal = sheets_service.get_account_balance(
                        final_exchange_name, fee_asset_parsed, initial_balances_map)
                    return False, f"Недостаточно {fee_asset_parsed} на {final_exchange_name} для комиссии. Нужно: {_format_for_user_message(fee_amount, fee_asset_parsed)}, доступно: {_format_for_user_message(cur_bal, fee_asset_parsed)}."
                updates_for_balances.append(
                    {'account': final_exchange_name, 'asset': fee_asset_parsed, 'change': -fee_amount})

        potential_risk_usd = None
        stop_loss_str = optional_fields.get('sl')
        if trade_type_upper == 'BUY' and stop_loss_str and price and qty:
            try:
                sl_price = _safe_decimal(
                    stop_loss_str, PRICE_QUANTIZER_LOGGING)
                if sl_price is not None and sl_price < price:
                    potential_risk_usd = (price - sl_price) * qty
                    potential_risk_usd = potential_risk_usd.quantize(
                        USD_QUANTIZER_LOGGING, rounding=ROUND_HALF_UP)
                    logger.info(
                        f"Рассчитан потенциальный риск: {potential_risk_usd} USD")
                elif sl_price is not None and sl_price >= price:
                    logger.warning(
                        f"Stop Loss ({sl_price}) для BUY-сделки выше или равен цене входа ({price}). Риск не рассчитан.")
            except (InvalidOperation, TypeError):
                logger.error(f"Ошибка расчета риска для SL '{stop_loss_str}'.")

        calculated_trade_pnl = None
        if trade_type_upper == 'SELL':
            op_row_idx, existing_op_data = sheets_service.find_position_by_symbol(
                symbol, final_exchange_name)
            if existing_op_data:
                try:
                    avg_entry_price_str = existing_op_data.get(
                        'Avg_Entry_Price')
                    avg_entry_price_dec = _safe_decimal(
                        avg_entry_price_str, PRICE_QUANTIZER_LOGGING)

                    sell_price_dec = price
                    sell_qty_dec = qty

                    if all([avg_entry_price_dec is not None, sell_price_dec is not None, sell_qty_dec is not None]):
                        calculated_trade_pnl = (
                            sell_price_dec - avg_entry_price_dec) * sell_qty_dec

                        pnl_quantizer = USD_QUANTIZER_LOGGING
                        if quote_asset not in getattr(config, 'INVESTMENT_ASSETS', ['USD', 'USDT']):
                            logger.warning(
                                f"Расчет Trade_PNL для пары с не-стейбл квотой {quote_asset}.")

                        calculated_trade_pnl = calculated_trade_pnl.quantize(
                            pnl_quantizer, rounding=ROUND_HALF_UP)

                        logger.info(
                            f"Рассчитан Trade_PNL для продажи {symbol}: {calculated_trade_pnl} {quote_asset}. "
                            f"(Цена продажи: {sell_price_dec}, Сред. цена входа: {avg_entry_price_dec}, Кол-во: {sell_qty_dec})")
                    else:
                        logger.warning(
                            f"Не удалось рассчитать Trade_PNL для продажи {symbol}: один из компонентов пуст.")
                except Exception as pnl_e:
                    logger.error(
                        f"Критическая ошибка при расчете PNL для {symbol}: {pnl_e}", exc_info=True)
            else:
                logger.warning(
                    f"Не удалось рассчитать Trade_PNL: открытая позиция {symbol} на {final_exchange_name} не найдена.")

        final_notes = optional_fields.get('notes', '')
        if isinstance(final_notes, str) and final_notes.startswith("'") and final_notes.endswith("'") and len(final_notes) > 1:
            final_notes = final_notes[1:-1]

        core_trade_data_map = {
            'Timestamp': timestamp_str, 'Order_ID': optional_fields.get('id', ''), 'Exchange': final_exchange_name,
            'Symbol': symbol.upper(), 'Type': trade_type_upper, 'Amount': qty, 'Price': price,
            'Total_Quote_Amount': total_quote_amount,
            'TP1': _safe_decimal(optional_fields.get('tp1'), PRICE_QUANTIZER_LOGGING, None),
            'TP2': _safe_decimal(optional_fields.get('tp2'), PRICE_QUANTIZER_LOGGING, None),
            'TP3': _safe_decimal(optional_fields.get('tp3'), PRICE_QUANTIZER_LOGGING, None),
            'SL': _safe_decimal(optional_fields.get('sl'), PRICE_QUANTIZER_LOGGING, None),
            'Risk_USD': potential_risk_usd, 'Strategy': optional_fields.get('strat', ''), 'Trade_PNL': calculated_trade_pnl,
            'Commission': fee_amount if fee_amount > Decimal('0') else None,
            'Commission_Asset': fee_asset_parsed if fee_amount > Decimal('0') else None,
            'Source': source, 'Asset_Type': str(optional_fields.get('asset_type', asset_type)).strip().upper(),
            'Notes': final_notes, 'Fifo_Consumed_Qty': Decimal('0') if trade_type_upper == 'BUY' else None,
            'Fifo_Sell_Processed': 'FALSE' if trade_type_upper == 'SELL' else None,
            'Trade_ID': generated_trade_id
        }
        core_trades_headers = sheets_service.get_headers(
            config.CORE_TRADES_SHEET_NAME)
        if not core_trades_headers:
            return False, "Не удалось получить заголовки Core_Trades."

        core_trade_row_final_list = [core_trade_data_map.get(
            header) for header in core_trades_headers]

        if not sheets_service.append_to_sheet(config.CORE_TRADES_SHEET_NAME, core_trade_row_final_list):
            return False, "Ошибка записи сделки в Core_Trades."

        logger.info(
            f"Подготовка к обновлению балансов для Trade_ID: {generated_trade_id}...")
        if not sheets_service.batch_update_balances(updates_for_balances, initial_balances_map):
            return False, "Ошибка обновления балансов после сделки."

        open_pos_sheet_name = config.OPEN_POSITIONS_SHEET_NAME

        exchange_key = final_exchange_name.lower()

        current_balance = initial_balances_map.get(
            (exchange_key, base_asset), {}).get('balance', Decimal('0'))

        change_for_base_asset = Decimal('0')
        for item in updates_for_balances:
            if item['account'].lower() == exchange_key and item['asset'] == base_asset:
                change_for_base_asset += item['change']

        calculated_new_base_asset_balance = (current_balance + change_for_base_asset).quantize(
            QTY_QUANTIZER_LOGGING, ROUND_HALF_UP)

        logger.info(
            f"[OpenPos Sync] Расчетный новый баланс {base_asset} на {final_exchange_name}: {calculated_new_base_asset_balance}")

        op_row_index, existing_op = sheets_service.find_position_by_symbol(
            symbol, final_exchange_name)
        zero_threshold_op = QTY_QUANTIZER_LOGGING / Decimal('10000')

        if calculated_new_base_asset_balance <= zero_threshold_op:
            if existing_op and op_row_index:
                logger.info(
                    f"[OpenPos Sync] Удаляем позицию {symbol} (строка {op_row_index}).")
                sheets_service.delete_row_from_sheet(
                    open_pos_sheet_name, op_row_index, f"Закрытие позиции {symbol}")
            else:
                logger.info(
                    f"[OpenPos Sync] Баланс {base_asset} ({calculated_new_base_asset_balance}) нулевой или ниже порога. Записи в Open_Positions для удаления не найдено.")
        else:
            actual_op_net_amount = calculated_new_base_asset_balance
            if trade_type_upper == 'BUY':
                new_avg_price = price
                if existing_op and op_row_index:
                    old_net_amount = _safe_decimal(existing_op.get(
                        'Net_Amount'), QTY_QUANTIZER_LOGGING, Decimal('0'))
                    old_avg_price = _safe_decimal(existing_op.get(
                        'Avg_Entry_Price'), PRICE_QUANTIZER_LOGGING, Decimal('0'))

                    if old_net_amount is not None and old_avg_price is not None and actual_op_net_amount > zero_threshold_op:
                        new_avg_price = (
                            (old_net_amount * old_avg_price) + (qty * price)) / actual_op_net_amount
                        new_avg_price = new_avg_price.quantize(
                            PRICE_QUANTIZER_LOGGING, ROUND_HALF_UP)

                    logger.info(
                        f"[OpenPos Sync] Обновление BUY {symbol}: NetAmt={actual_op_net_amount}, AvgPrice={new_avg_price}")
                    sheets_service.update_open_position_entry(
                        op_row_index, actual_op_net_amount, new_avg_price, final_exchange_name, symbol)
                else:
                    logger.info(
                        f"[OpenPos Sync] Создание новой BUY позиции {symbol}: NetAmt={actual_op_net_amount}, AvgPrice={price}")
                    op_headers = sheets_service.get_headers(
                        open_pos_sheet_name)
                    if not op_headers:
                        return False, f"Нет заголовков {open_pos_sheet_name}."

                    new_pos_data_map = {
                        'Symbol': symbol.upper(), 'Exchange': final_exchange_name, 'Net_Amount': actual_op_net_amount,
                        'Avg_Entry_Price': price, 'Current_Price': None, 'Unrealized_PNL': None,
                        'Last_Updated': datetime.now(timezone.utc).astimezone(target_timezone).strftime("%Y-%m-%d %H:%M:%S")
                    }
                    new_pos_row_values = [new_pos_data_map.get(
                        header) for header in op_headers]

                    if not sheets_service.append_to_sheet(open_pos_sheet_name, new_pos_row_values):
                        logger.error(
                            f"[OpenPos Sync] Ошибка записи новой позиции {symbol} в {open_pos_sheet_name}")
                    else:
                        logger.info(
                            f"[OpenPos Sync] Новая позиция {symbol} успешно добавлена в {open_pos_sheet_name}")
            elif trade_type_upper == 'SELL':
                if existing_op and op_row_index:
                    current_op_avg_price = _safe_decimal(existing_op.get(
                        'Avg_Entry_Price'), PRICE_QUANTIZER_LOGGING, Decimal('0'))
                    logger.info(
                        f"[OpenPos Sync] Обновление SELL {symbol}: NetAmt={actual_op_net_amount}, AvgPrice={current_op_avg_price if current_op_avg_price is not None else 'N/A'}")
                    sheets_service.update_open_position_entry(
                        op_row_index, actual_op_net_amount, current_op_avg_price if current_op_avg_price is not None else Decimal('0'), final_exchange_name, symbol)
                else:
                    logger.warning(
                        f"[OpenPos Sync] Продажа {symbol} на {final_exchange_name}, но позиция не найдена в Open_Positions.")

        logger.info(
            f"Сделка {trade_type_upper} {symbol} и балансы успешно обновлены. ID: {generated_trade_id}")
        return True, generated_trade_id
    except Exception as e:
        logger.error(f"Критическая ошибка в log_trade: {e}", exc_info=True)
        return False, "Внутренняя ошибка при логировании сделки."


def log_fund_movement(
    movement_type: str, asset: str, amount_str: str,
    source_entity_type: str | None, source_name: str | None,
    destination_entity_type: str | None, destination_name: str | None,
    fee_amount_str: str | None = None,
    fee_asset: str | None = None,
    transaction_id_blockchain: str | None = None, notes: str | None = None,
    movement_timestamp_obj: datetime | None = None
) -> tuple[bool, str | None]:

    # ИСПРАВЛЕНО: Приводим имена счетов к нижнему регистру в самом начале
    final_source_name = source_name.strip().lower() if source_name else None
    final_destination_name = destination_name.strip(
    ).lower() if destination_name else None

    move_type_upper = movement_type.upper()
    log_context = f"type={move_type_upper}, asset={asset}, amount={amount_str}"
    logger.info(f"Начало log_fund_movement: {log_context}")

    try:
        movement_id = str(uuid.uuid4())
        target_timezone = timezone(timedelta(hours=config.TZ_OFFSET_HOURS))
        final_timestamp_str: str
        if movement_timestamp_obj:
            aware_dt = movement_timestamp_obj.replace(tzinfo=target_timezone) if movement_timestamp_obj.tzinfo is None \
                else movement_timestamp_obj.astimezone(target_timezone)
            final_timestamp_str = aware_dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            final_timestamp_str = datetime.now(timezone.utc).astimezone(
                target_timezone).strftime("%Y-%m-%d %H:%M:%S")

        asset_upper = str(asset).upper()
        amount_quantizer = USD_QUANTIZER_LOGGING if asset_upper in getattr(
            config, 'INVESTMENT_ASSETS', ['USD', 'USDT']) else QTY_QUANTIZER_LOGGING

        amount_decimal = _safe_decimal(
            amount_str, amount_quantizer, default_if_none=None)
        if amount_decimal is None or amount_decimal <= Decimal(0):
            return False, f"Некорректная или нулевая сумма '{amount_str}'."

        fee_amount_decimal, final_fee_asset = Decimal('0'), None
        if fee_amount_str:
            fee_asset_candidate = str(fee_asset or asset).upper()
            fee_quantizer = USD_QUANTIZER_LOGGING if fee_asset_candidate in getattr(
                config, 'INVESTMENT_ASSETS', ['USD', 'USDT']) else QTY_QUANTIZER_LOGGING
            temp_fee = _safe_decimal(
                fee_amount_str, fee_quantizer, Decimal('0'))
            if temp_fee and temp_fee > Decimal('0'):
                fee_amount_decimal, final_fee_asset = temp_fee, fee_asset_candidate

        final_movement_notes = str(notes).strip() if notes else ''
        if isinstance(final_movement_notes, str) and final_movement_notes.startswith("'") and final_movement_notes.endswith("'") and len(final_movement_notes) > 1:
            final_movement_notes = final_movement_notes[1:-1]

        initial_balances_map_fm = sheets_service.get_all_balances()
        if initial_balances_map_fm is None:
            logger.error(
                f"Не удалось загрузить балансы для log_fund_movement ({log_context}).")
            return False, "Внутренняя ошибка: не удалось загрузить балансы."

        effective_source_type = str(source_entity_type).upper(
        ).strip() if source_entity_type else ""
        effective_dest_type = str(destination_entity_type).upper(
        ).strip() if destination_entity_type else ""

        logger.info(f"Детали операции {movement_id}: "
                    f"Ист: '{final_source_name}' (тип: {effective_source_type}), "
                    f"Назн: '{final_destination_name}' (тип: {effective_dest_type})")

        if (move_type_upper == "WITHDRAWAL" or move_type_upper == "TRANSFER") and \
           final_source_name and effective_source_type != 'EXTERNAL':
            required_main = amount_decimal
            required_fee = fee_amount_decimal
            if final_fee_asset == asset_upper and fee_amount_decimal > Decimal('0'):
                required_main += required_fee
                required_fee = Decimal('0')

            if not sheets_service.has_sufficient_balance(final_source_name, asset_upper, required_main, balances_map=initial_balances_map_fm):
                cur_bal = sheets_service.get_account_balance(
                    final_source_name, asset_upper, initial_balances_map_fm)
                return False, f"Недостаточно {asset_upper} на счете {final_source_name}. Нужно: {_format_for_user_message(required_main, asset_upper)}, доступно: {_format_for_user_message(cur_bal, asset_upper)}."
            if required_fee > Decimal('0') and final_fee_asset:
                if not sheets_service.has_sufficient_balance(final_source_name, final_fee_asset, required_fee, balances_map=initial_balances_map_fm):
                    cur_bal = sheets_service.get_account_balance(
                        final_source_name, final_fee_asset, initial_balances_map_fm)
                    return False, f"Недостаточно {final_fee_asset} на счете {final_source_name} для комиссии. Нужно: {_format_for_user_message(required_fee, final_fee_asset)}, доступно: {_format_for_user_message(cur_bal, final_fee_asset)}."

        fund_movement_data_map = {
            'Movement_ID': movement_id, 'Timestamp': final_timestamp_str, 'Type': move_type_upper,
            'Asset': asset_upper, 'Amount': amount_decimal,
            'Source_Entity_Type': effective_source_type if effective_source_type else None,
            'Source_Name': final_source_name,
            'Destination_Entity_Type': effective_dest_type if effective_dest_type else None,
            'Destination_Name': final_destination_name,
            'Fee_Amount': fee_amount_decimal if fee_amount_decimal > Decimal('0') else None,
            'Fee_Asset': final_fee_asset if fee_amount_decimal > Decimal('0') else None,
            'Transaction_ID_Blockchain': str(transaction_id_blockchain).strip() if transaction_id_blockchain else None,
            'Notes': final_movement_notes
        }

        fund_movements_headers = sheets_service.get_headers(
            config.FUND_MOVEMENTS_SHEET_NAME)
        if not fund_movements_headers:
            return False, "Ошибка: не найдены заголовки Fund_Movements."

        data_row_final_list = [fund_movement_data_map.get(
            header) for header in fund_movements_headers]

        if not sheets_service.append_to_sheet(config.FUND_MOVEMENTS_SHEET_NAME, data_row_final_list):
            logger.error(
                f"Ошибка записи движения средств в таблицу для ID: {movement_id}")
            return False, "Ошибка записи движения средств в таблицу."

        logger.info(
            f"Движение средств успешно записано в Fund_Movements для ID: {movement_id}")

        updates_for_balances = []
        if final_source_name and effective_source_type != 'EXTERNAL':
            updates_for_balances.append(
                {'account': final_source_name, 'asset': asset_upper, 'change': -amount_decimal})
            if fee_amount_decimal > Decimal('0') and final_fee_asset:
                updates_for_balances.append(
                    {'account': final_source_name, 'asset': final_fee_asset, 'change': -fee_amount_decimal})

        if final_destination_name and effective_dest_type != 'EXTERNAL':
            updates_for_balances.append(
                {'account': final_destination_name, 'asset': asset_upper, 'change': amount_decimal})

        if updates_for_balances:
            logger.info(
                f"Подготовка к обновлению балансов для Movement_ID: {movement_id}. Изменения: {updates_for_balances}")
            if not sheets_service.batch_update_balances(updates_for_balances, initial_balances_map_fm):
                logger.critical(
                    f"Движение залогировано в Fund_Movements (ID: {movement_id}), но НЕ УДАЛОСЬ обновить балансы! ТРЕБУЕТСЯ РУЧНАЯ ПРОВЕРКА!")
                return False, "Критическая ошибка: движение записано, но балансы не обновлены."
            logger.info(
                f"Балансы для Movement_ID: {movement_id} успешно обновлены.")
        else:
            logger.info(
                f"Для Movement_ID: {movement_id} не требуется обновление балансов (внешняя операция).")

        logger.info(
            f"Движение средств и обновление балансов (если требовалось) успешно залогированы. Movement_ID: {movement_id}")
        return True, movement_id
    except Exception as e:
        logger.error(
            f"Критическая ошибка в log_fund_movement ({log_context}): {e}", exc_info=True)
        return False, "Внутренняя ошибка при логировании движения."
