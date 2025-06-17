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
    trade_type: str, symbol: str, qty_str: str, price_str: str,
    exchange_name: str,
    named_args: dict,
    trade_timestamp_obj: datetime | None = None
) -> tuple[bool, str | None]:
    generated_trade_id = str(uuid.uuid4())
    target_timezone = timezone(timedelta(hours=config.TZ_OFFSET_HOURS))

    final_timestamp_obj = trade_timestamp_obj if trade_timestamp_obj else datetime.now(
        timezone.utc)

    if final_timestamp_obj.tzinfo is None:
        final_timestamp_obj = final_timestamp_obj.replace(
            tzinfo=target_timezone)
    else:
        final_timestamp_obj = final_timestamp_obj.astimezone(target_timezone)

    timestamp_str = final_timestamp_obj.strftime("%Y-%m-%d %H:%M:%S")

    try:
        qty = _safe_decimal(qty_str, QTY_QUANTIZER_LOGGING)
        price = _safe_decimal(price_str, PRICE_QUANTIZER_LOGGING)
        base_asset, quote_asset = _parse_symbol(symbol)

        if not all([qty, price, base_asset, quote_asset]) or qty <= Decimal('0') or price <= Decimal('0'):
            return False, "Количество, цена или символ указаны некорректно."

        final_exchange_name = exchange_name.strip().lower()

        initial_balances_map = sheets_service.get_all_balances()
        if initial_balances_map is None:
            return False, "Внутренняя ошибка: не удалось загрузить балансы."

        total_quote_amount = (
            qty * price).quantize(USD_QUANTIZER_LOGGING, rounding=ROUND_HALF_UP)
        fee_amt_str = named_args.get('fee', named_args.get('com'))
        fee_asset_str = named_args.get('fee_asset')
        fee_amount = Decimal('0')
        fee_asset_parsed = None
        if fee_amt_str:
            fee_asset_candidate = (fee_asset_str or quote_asset).upper()
            fee_quantizer = USD_QUANTIZER_LOGGING if fee_asset_candidate == config.BASE_CURRENCY else QTY_QUANTIZER_LOGGING
            temp_fee = _safe_decimal(fee_amt_str, fee_quantizer, Decimal('0'))
            if temp_fee and temp_fee > Decimal('0'):
                fee_amount, fee_asset_parsed = temp_fee, fee_asset_candidate

        updates_for_balances = []
        trade_type_upper = trade_type.upper()

        if trade_type_upper == 'BUY':
            cost_main = total_quote_amount
            if fee_asset_parsed == quote_asset:
                cost_main += fee_amount
            if not sheets_service.has_sufficient_balance(final_exchange_name, quote_asset, cost_main, initial_balances_map):
                return False, f"Недостаточно {quote_asset} на {final_exchange_name}."
            updates_for_balances.append(
                {'account': final_exchange_name, 'asset': quote_asset, 'change': -cost_main})
            if fee_amount > Decimal('0') and fee_asset_parsed != quote_asset:
                if not sheets_service.has_sufficient_balance(final_exchange_name, fee_asset_parsed, fee_amount, initial_balances_map):
                    return False, f"Недостаточно {fee_asset_parsed} для комиссии."
                updates_for_balances.append(
                    {'account': final_exchange_name, 'asset': fee_asset_parsed, 'change': -fee_amount})
            updates_for_balances.append(
                {'account': final_exchange_name, 'asset': base_asset, 'change': qty})

        elif trade_type_upper == 'SELL':
            cost_main = qty
            if fee_asset_parsed == base_asset:
                cost_main += fee_amount
            if not sheets_service.has_sufficient_balance(final_exchange_name, base_asset, cost_main, initial_balances_map):
                return False, f"Недостаточно {base_asset} на {final_exchange_name}."
            updates_for_balances.append(
                {'account': final_exchange_name, 'asset': base_asset, 'change': -cost_main})
            proceeds_quote = total_quote_amount
            if fee_asset_parsed == quote_asset:
                proceeds_quote -= fee_amount
            updates_for_balances.append(
                {'account': final_exchange_name, 'asset': quote_asset, 'change': proceeds_quote})
            if fee_amount > Decimal('0') and fee_asset_parsed and fee_asset_parsed != base_asset and fee_asset_parsed != quote_asset:
                if not sheets_service.has_sufficient_balance(final_exchange_name, fee_asset_parsed, fee_amount, initial_balances_map):
                    return False, f"Недостаточно {fee_asset_parsed} для комиссии."
                updates_for_balances.append(
                    {'account': final_exchange_name, 'asset': fee_asset_parsed, 'change': -fee_amount})

        final_notes = named_args.get('notes', '')

        core_trade_data_map = {
            'Timestamp': timestamp_str, 'Order_ID': named_args.get('id', ''), 'Exchange': final_exchange_name,
            'Symbol': symbol.upper(), 'Type': trade_type_upper, 'Amount': qty, 'Price': price,
            'Commission': fee_amount if fee_amount > Decimal('0') else None,
            'Commission_Asset': fee_asset_parsed if fee_amount > Decimal('0') else None,
            'Notes': final_notes, 'Trade_ID': generated_trade_id
            # Поля PNL и другие будут вычисляться позже, здесь не указываем
        }
        core_trades_headers = sheets_service.get_headers(
            config.CORE_TRADES_SHEET_NAME)
        if not core_trades_headers:
            return False, "Не удалось получить заголовки Core_Trades."

        core_trade_row_list = [core_trade_data_map.get(
            header) for header in core_trades_headers]

        # ++ ИЗМЕНЕНИЕ: Явное преобразование Decimal в строку перед записью в лист ++
        final_row_for_sheet = []
        for item in core_trade_row_list:
            if isinstance(item, Decimal):
                final_row_for_sheet.append(str(item))
            else:
                final_row_for_sheet.append(item)

        if not sheets_service.append_to_sheet(config.CORE_TRADES_SHEET_NAME, final_row_for_sheet):
            return False, "Ошибка записи сделки в Core_Trades."

        if not sheets_service.batch_update_balances(updates_for_balances, initial_balances_map):
            return False, "Ошибка обновления балансов после сделки."

        # Логика обновления открытых позиций (без изменений)
        op_row_index, existing_op = sheets_service.find_position_by_symbol(
            symbol, final_exchange_name)
        if trade_type_upper == 'BUY':
            if existing_op:
                old_net_amount = _safe_decimal(
                    existing_op.get('Net_Amount'), QTY_QUANTIZER_LOGGING)
                old_avg_price = _safe_decimal(existing_op.get(
                    'Avg_Entry_Price'), PRICE_QUANTIZER_LOGGING)
                new_total_amount = old_net_amount + qty
                new_avg_price = ((old_net_amount * old_avg_price) +
                                 (qty * price)) / new_total_amount
                sheets_service.update_open_position_entry(
                    op_row_index, new_total_amount, new_avg_price, final_exchange_name, symbol)
            else:
                sheets_service.add_new_open_position(
                    symbol, final_exchange_name, qty, price)
        elif trade_type_upper == 'SELL':
            if existing_op:
                old_net_amount = _safe_decimal(
                    existing_op.get('Net_Amount'), QTY_QUANTIZER_LOGGING)
                new_total_amount = old_net_amount - qty
                if new_total_amount <= (QTY_QUANTIZER_LOGGING / 100):
                    sheets_service.delete_row_from_sheet(
                        config.OPEN_POSITIONS_SHEET_NAME, op_row_index, f"Закрытие позиции {symbol}")
                else:
                    old_avg_price = _safe_decimal(existing_op.get(
                        'Avg_Entry_Price'), PRICE_QUANTIZER_LOGGING)
                    sheets_service.update_open_position_entry(
                        op_row_index, new_total_amount, old_avg_price, final_exchange_name, symbol)

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

    final_source_name = source_name.strip().lower() if source_name else None
    final_destination_name = destination_name.strip(
    ).lower() if destination_name else None

    move_type_upper = movement_type.upper()
    try:
        movement_id = str(uuid.uuid4())
        target_timezone = timezone(timedelta(hours=config.TZ_OFFSET_HOURS))
        if movement_timestamp_obj:
            aware_dt = movement_timestamp_obj.replace(
                tzinfo=target_timezone) if movement_timestamp_obj.tzinfo is None else movement_timestamp_obj.astimezone(target_timezone)
            final_timestamp_str = aware_dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            final_timestamp_str = datetime.now(timezone.utc).astimezone(
                target_timezone).strftime("%Y-%m-%d %H:%M:%S")

        asset_upper = str(asset).upper()
        amount_quantizer = USD_QUANTIZER_LOGGING if asset_upper in getattr(
            config, 'INVESTMENT_ASSETS', []) else QTY_QUANTIZER_LOGGING
        amount_decimal = _safe_decimal(amount_str, amount_quantizer)
        if amount_decimal is None or amount_decimal <= Decimal(0):
            return False, f"Некорректная сумма '{amount_str}'."

        fee_amount_decimal, final_fee_asset = Decimal('0'), None
        if fee_amount_str:
            fee_asset_candidate = str(fee_asset or asset).upper()
            fee_quantizer_fee = USD_QUANTIZER_LOGGING if fee_asset_candidate in getattr(
                config, 'INVESTMENT_ASSETS', []) else QTY_QUANTIZER_LOGGING
            temp_fee = _safe_decimal(fee_amount_str, fee_quantizer_fee)
            if temp_fee and temp_fee > Decimal('0'):
                fee_amount_decimal, final_fee_asset = temp_fee, fee_asset_candidate

        initial_balances_map_fm = sheets_service.get_all_balances()
        if initial_balances_map_fm is None:
            return False, "Внутренняя ошибка: не удалось загрузить балансы."

        updates = []
        if final_source_name and source_entity_type != 'EXTERNAL':
            updates.append({'account': final_source_name,
                           'asset': asset_upper, 'change': -amount_decimal})
            if fee_amount_decimal > 0 and final_fee_asset:
                updates.append({'account': final_source_name,
                               'asset': final_fee_asset, 'change': -fee_amount_decimal})

        if final_destination_name and destination_entity_type != 'EXTERNAL':
            updates.append({'account': final_destination_name,
                           'asset': asset_upper, 'change': amount_decimal})

        fund_movement_data_map = {
            'Movement_ID': movement_id, 'Timestamp': final_timestamp_str, 'Type': move_type_upper,
            'Asset': asset_upper, 'Amount': str(amount_decimal),
            'Source_Entity_Type': source_entity_type, 'Source_Name': final_source_name,
            'Destination_Entity_Type': destination_entity_type, 'Destination_Name': final_destination_name,
            'Fee_Amount': str(fee_amount_decimal) if fee_amount_decimal > 0 else None,
            'Fee_Asset': final_fee_asset if fee_amount_decimal > 0 else None,
            'Transaction_ID_Blockchain': transaction_id_blockchain, 'Notes': notes
        }
        headers = sheets_service.get_headers(config.FUND_MOVEMENTS_SHEET_NAME)
        if not headers:
            return False, "Ошибка: не найдены заголовки Fund_Movements."

        row_to_write = [fund_movement_data_map.get(h) for h in headers]

        if not sheets_service.append_to_sheet(config.FUND_MOVEMENTS_SHEET_NAME, row_to_write):
            return False, "Ошибка записи движения средств в таблицу."

        if updates and not sheets_service.batch_update_balances(updates, initial_balances_map_fm):
            return False, "Критическая ошибка: движение записано, но балансы не обновлены."

        return True, movement_id
    except Exception as e:
        logger.error(
            f"Критическая ошибка в log_fund_movement: {e}", exc_info=True)
        return False, "Внутренняя ошибка при логировании движения."
