# deal_tracker/telegram_handlers.py
from telegram_parser import parse_command_args_advanced
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
import re
import gspread

from telegram import Update
from telegram.ext import CallbackContext
from telegram.constants import ParseMode

import config
import utils
from trade_logger import log_trade, log_fund_movement
import sheets_service

logger = logging.getLogger(__name__)


def _determine_entity_type(name: str, default_type_if_unknown="EXTERNAL") -> str:
    """
    Определяет тип сущности (биржа, кошелек, внешняя) по имени.
    Устойчива к пробелам в начале/конце и регистру.
    """
    if not name:
        return default_type_if_unknown

    name_lower = name.strip().lower()

    known_exchanges = getattr(config, 'KNOWN_EXCHANGES', [])
    known_wallets = getattr(config, 'KNOWN_WALLETS', [])

    if name_lower in [exch.strip().lower() for exch in known_exchanges if isinstance(exch, str)]:
        return "EXCHANGE"
    if name_lower in [w.strip().lower() for w in known_wallets if isinstance(w, str)]:
        return "WALLET"

    return default_type_if_unknown


def parse_command_args_advanced(args: list[str], num_positional_max: int) -> tuple[list[str], dict[str, str]]:
    positional_args = []
    named_args_dict = {}
    arg_idx = 0
    key_regex = r"^([a-zA-Z_а-яА-Я][a-zA-Z0-9_а-яА-Я]*):(.*)$"
    while arg_idx < len(args):
        current_token = args[arg_idx]
        if re.match(key_regex, current_token):
            break
        if len(positional_args) >= num_positional_max:
            break
        positional_args.append(current_token)
        arg_idx += 1
    current_key = None
    value_buffer = []
    while arg_idx < len(args):
        token = args[arg_idx]
        key_match = re.match(key_regex, token)
        if key_match:
            if current_key and value_buffer:
                named_args_dict[current_key] = " ".join(value_buffer).strip()
            current_key = key_match.group(1).lower()
            value_part = key_match.group(2).strip()
            value_buffer = []
            if value_part:
                if (value_part.startswith('"') and value_part.endswith('"')) or \
                   (value_part.startswith("'") and value_part.endswith("'")):
                    value_buffer.append(value_part[1:-1])
                else:
                    value_buffer.append(value_part)
        elif current_key:
            value_buffer.append(token)
        else:
            logger.warning(
                f"Нераспознанный токен при парсинге аргументов: '{token}'. Игнорируется.")
        arg_idx += 1
    if current_key and value_buffer:
        named_args_dict[current_key] = " ".join(value_buffer).strip()
    elif current_key and current_key not in named_args_dict:
        named_args_dict[current_key] = ""
    return positional_args, named_args_dict


def admin_only(func):
    async def wrapped(update: Update, context: CallbackContext, *args, **kwargs):
        user = update.effective_user
        user_id_str = str(user.id)
        admin_ids_str = getattr(config, 'TELEGRAM_ADMIN_IDS_STR', None)
        if not admin_ids_str:
            admin_ids_str = getattr(config, 'TELEGRAM_CHAT_ID', '')

        admin_ids = [s.strip() for s in admin_ids_str.split(',') if s.strip()]

        if user_id_str not in admin_ids:
            logger.warning(
                f"Неавторизованный доступ к команде '{func.__name__}' от user ID {user_id_str} (username: {user.username or 'N/A'}).")
            await update.message.reply_text("⛔️ У вас нет прав для выполнения этой команды.")
            return
        return await func(update, context, *args, **kwargs)
    return wrapped


async def start_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    logger.info(
        f"Command '/start' от user {user.id} ({user.username or 'N/A'}). Текст: '{update.message.text}'")
    user_name = user.first_name
    help_text = (
        f"Привет, {user_name}!\n"
        "Я бот для учета ваших криптовалютных сделок и финансов.\n\n"
        "<b>Доступные команды:</b>\n"
        "/help - Показать это сообщение\n"
        "--- <u>Торговля</u> ---\n"
        "<code>/buy SYMBOL QTY PRICE [SOURCE] [ключ:значение ...]</code>\n"
        "<code>/sell SYMBOL QTY PRICE [SOURCE] [ключ:значение ...]</code>\n"
        "  <i>Опц. ключи: <b>exch</b>, strat, tp1, sl, <b>fee</b>, <b>fee_asset</b>, id, notes, date, asset_type</i>\n"
        "  <i>Формат date: <code>date:ГГГГ-ММ-ДД</code> или <code>date:ГГГГ-ММ-ДД ЧЧ:ММ[:СС]</code></i>\n"
        "--- <u>Финансы</u> ---\n"
        "<code>/deposit ASSET AMOUNT [dest_name:КУДА] [ключи...]</code>\n"
        "<code>/withdraw ASSET AMOUNT [source_name:ОТКУДА] [ключи...]</code>\n"
        "<code>/transfer ASSET QTY FROM TO [ключи...]</code>\n"
        "  <i>Опц. ключи: date, notes, tx_id, fee, fee_asset</i>\n"
        "--- <u>Отчеты</u> ---\n"
        "/portfolio - Открытые позиции\n"
        "/history SYMBOL - История сделок по символу\n"
        "/average SYMBOL - Средняя цена входа по символу\n"
        "/movements - Детальное движение средств\n"
        "/update_analytics - Обновить аналитику и FIFO\n"
        "/updater_status - Статус обновления цен\n"
    )
    await update.message.reply_text(help_text, parse_mode=ParseMode.HTML)


async def help_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    logger.info(
        f"Command '/help' от user {user.id} ({user.username or 'N/A'}). Текст: '{update.message.text}'")
    await start_command(update, context)


@admin_only
async def buy_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    command_text = update.message.text
    logger.info(
        f"Command '/buy' (текст: '{command_text}') от user {user.id} ({user.username or 'N/A'}). Args: {context.args}")

    args = list(context.args)
    num_expected_positional_min = 3
    num_expected_positional_max = 4
    positional_args, named_args = parse_command_args_advanced(
        args, num_expected_positional_max)

    if len(positional_args) < num_expected_positional_min:
        await update.message.reply_text("Ошибка: <code>/buy SYMBOL QTY PRICE [SOURCE] [ключи...]</code>", parse_mode=ParseMode.HTML)
        return

    symbol = positional_args[0]
    qty_str = positional_args[1]
    price_str = positional_args[2]
    source = positional_args[3] if len(positional_args) == num_expected_positional_max else getattr(
        config, 'DEFAULT_MANUAL_TRADE_SOURCE', "manual")
    exchange_position_name = named_args.get('exch', named_args.get('exchange'))

    if not exchange_position_name:
        await update.message.reply_text("❌ Ошибка: Для команды /buy **обязательно** нужно указать биржу через `exch:ИМЯ_БИРЖИ`.", parse_mode=ParseMode.HTML)
        return

    strategy_position_name = named_args.get(
        'strat', named_args.get('strategy'))
    order_id = named_args.get('id', named_args.get('order_id'))
    asset_type = named_args.get('asset_type', "SPOT")

    logger.info(
        f"Обработка /buy для user {user.id}: symbol='{symbol}', qty='{qty_str}', price='{price_str}', source='{source}', exch='{exchange_position_name}', named_args={named_args}")

    success, result_msg_or_id = log_trade("BUY", symbol, qty_str, price_str, source,
                                          exchange_position_name, strategy_position_name, named_args, order_id, asset_type)
    if success:
        await update.message.reply_text(f"✅ Покупка {qty_str} {symbol} @ {price_str} залогирована. ID: <code>{result_msg_or_id}</code>", parse_mode=ParseMode.HTML)
    else:
        error_to_show = result_msg_or_id if isinstance(
            result_msg_or_id, str) else "Ошибка логирования покупки. Проверьте логи."
        logger.error(
            f"Ошибка логирования /buy для user {user.id} (symbol: {symbol}, qty: {qty_str}, price: {price_str}): {error_to_show}")
        await update.message.reply_text(f"❌ {error_to_show}", parse_mode=ParseMode.HTML)


@admin_only
async def sell_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    command_text = update.message.text
    logger.info(
        f"Command '/sell' (текст: '{command_text}') от user {user.id} ({user.username or 'N/A'}). Args: {context.args}")

    args = list(context.args)
    num_expected_positional_min = 3
    num_expected_positional_max = 4
    positional_args, named_args = parse_command_args_advanced(
        args, num_expected_positional_max)

    if len(positional_args) < num_expected_positional_min:
        await update.message.reply_text("Ошибка: <code>/sell SYMBOL QTY PRICE [SOURCE] [ключи...]</code>", parse_mode=ParseMode.HTML)
        return

    symbol = positional_args[0]
    qty_str = positional_args[1]
    price_str = positional_args[2]
    source = positional_args[3] if len(positional_args) == num_expected_positional_max else getattr(
        config, 'DEFAULT_MANUAL_TRADE_SOURCE', "manual")
    exchange_position_name = named_args.get('exch', named_args.get('exchange'))

    if not exchange_position_name:
        await update.message.reply_text("❌ Ошибка: Для команды /sell **обязательно** нужно указать биржу через `exch:ИМЯ_БИРЖИ`.", parse_mode=ParseMode.HTML)
        return

    strategy_position_name = named_args.get(
        'strat', named_args.get('strategy'))
    order_id = named_args.get('id', named_args.get('order_id'))
    asset_type = named_args.get('asset_type', "SPOT")

    logger.info(
        f"Обработка /sell для user {user.id}: symbol='{symbol}', qty='{qty_str}', price='{price_str}', source='{source}', exch='{exchange_position_name}', named_args={named_args}")

    success, result_msg_or_id = log_trade("SELL", symbol, qty_str, price_str, source,
                                          exchange_position_name, strategy_position_name, named_args, order_id, asset_type)
    if success:
        await update.message.reply_text(f"✅ Продажа {qty_str} {symbol} @ {price_str} залогирована. ID: <code>{result_msg_or_id}</code>", parse_mode=ParseMode.HTML)
    else:
        error_to_show = result_msg_or_id if isinstance(
            result_msg_or_id, str) else "Ошибка логирования продажи. Проверьте логи."
        logger.error(
            f"Ошибка логирования /sell для user {user.id} (symbol: {symbol}, qty: {qty_str}, price: {price_str}): {error_to_show}")
        await update.message.reply_text(f"❌ {error_to_show}", parse_mode=ParseMode.HTML)


@admin_only
async def deposit_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    command_text = update.message.text
    logger.info(
        f"Command '/deposit' (текст: '{command_text}') от user {user.id} ({user.username or 'N/A'}). Args: {context.args}")

    args = list(context.args)
    num_positional = 2
    positional_args, named_args = parse_command_args_advanced(
        args, num_positional)

    if len(positional_args) < num_positional:
        await update.message.reply_text("Использование: <code>/deposit ASSET AMOUNT [dest_name:КУДА] [ключи...]</code>", parse_mode=ParseMode.HTML)
        return

    asset_input, amount_str = positional_args
    asset = asset_input.upper()
    destination_name_input = named_args.get('dest_name')

    if not destination_name_input:
        await update.message.reply_text("❌ Ошибка: Укажите счет назначения через `dest_name:ИМЯ_СЧЕТА`.", parse_mode=ParseMode.HTML)
        return

    source_entity_type = "EXTERNAL"
    source_name = getattr(
        config, 'DEFAULT_DEPOSIT_SOURCE_NAME', "External Inflow")
    destination_entity_type = _determine_entity_type(
        destination_name_input, "INTERNAL_ACCOUNT")

    notes, tx_id = named_args.get("notes"), named_args.get("tx_id")
    fee_amount_str, fee_asset_str = named_args.get(
        'fee'), named_args.get('fee_asset')

    try:
        movement_timestamp = utils.parse_datetime_from_args(named_args)
        if Decimal(amount_str.replace(',', '.')) <= Decimal('0'):
            raise ValueError("Сумма должна быть больше 0.")
    except (InvalidOperation, ValueError) as e:
        await update.message.reply_text(f"❌ Ошибка в данных: {e}", parse_mode=ParseMode.HTML)
        return

    logger.info(
        f"Обработка /deposit для user {user.id}: asset='{asset}', amount='{amount_str}', dest_name='{destination_name_input}' (type: {destination_entity_type}), named_args='{named_args}'")

    success, result_msg_or_id = log_fund_movement(
        movement_type="DEPOSIT", asset=asset, amount_str=amount_str,
        source_entity_type=source_entity_type, source_name=source_name,
        destination_entity_type=destination_entity_type, destination_name=destination_name_input,
        fee_amount_str=fee_amount_str, fee_asset=fee_asset_str,
        transaction_id_blockchain=tx_id, notes=notes, movement_timestamp_obj=movement_timestamp)

    if success:
        await update.message.reply_text(f"✅ Депозит {amount_str} {asset} на '{destination_name_input}' залогирован. ID: <code>{result_msg_or_id}</code>", parse_mode=ParseMode.HTML)
    else:
        error_to_show = result_msg_or_id if isinstance(
            result_msg_or_id, str) else "Ошибка логирования депозита. Проверьте логи."
        logger.error(
            f"Ошибка логирования /deposit для user {user.id} (asset: {asset}, amount: {amount_str}, dest: {destination_name_input}): {error_to_show}")
        await update.message.reply_text(f"❌ {error_to_show}", parse_mode=ParseMode.HTML)


@admin_only
async def withdraw_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    command_text = update.message.text
    logger.info(
        f"Command '/withdraw' (текст: '{command_text}') от user {user.id} ({user.username or 'N/A'}). Args: {context.args}")

    args = list(context.args)
    num_positional = 2
    positional_args, named_args = parse_command_args_advanced(
        args, num_positional)

    if len(positional_args) < num_positional:
        await update.message.reply_text("Использование: <code>/withdraw ASSET AMOUNT [source_name:ОТКУДА] [ключи...]</code>", parse_mode=ParseMode.HTML)
        return

    asset_input, amount_str = positional_args
    asset = asset_input.upper()
    source_name_input = named_args.get('source_name')

    if not source_name_input:
        await update.message.reply_text("❌ Ошибка: Укажите счет списания через `source_name:ИМЯ_СЧЕТА`.", parse_mode=ParseMode.HTML)
        return

    source_entity_type = _determine_entity_type(
        source_name_input, "INTERNAL_ACCOUNT")
    destination_entity_type = "EXTERNAL"
    destination_name = getattr(
        config, 'DEFAULT_WITHDRAW_DESTINATION_NAME', "External Outflow")

    notes, tx_id = named_args.get("notes"), named_args.get("tx_id")
    fee_amount_str, fee_asset_str = named_args.get(
        'fee'), named_args.get('fee_asset')

    try:
        movement_timestamp = utils.parse_datetime_from_args(named_args)
        if Decimal(amount_str.replace(',', '.')) <= Decimal('0'):
            raise ValueError("Сумма должна быть больше 0.")
    except (InvalidOperation, ValueError) as e:
        await update.message.reply_text(f"❌ Ошибка в данных: {e}", parse_mode=ParseMode.HTML)
        return

    logger.info(
        f"Обработка /withdraw для user {user.id}: asset='{asset}', amount='{amount_str}', source_name='{source_name_input}' (type: {source_entity_type}), named_args='{named_args}'")

    success, result_msg_or_id = log_fund_movement(
        movement_type="WITHDRAWAL", asset=asset, amount_str=amount_str,
        source_entity_type=source_entity_type, source_name=source_name_input,
        destination_entity_type=destination_entity_type, destination_name=destination_name,
        fee_amount_str=fee_amount_str, fee_asset=fee_asset_str,
        transaction_id_blockchain=tx_id, notes=notes, movement_timestamp_obj=movement_timestamp)

    if success:
        await update.message.reply_text(f"✅ Снятие {amount_str} {asset} с '{source_name_input}' залогировано. ID: <code>{result_msg_or_id}</code>", parse_mode=ParseMode.HTML)
    else:
        error_to_show = result_msg_or_id if isinstance(
            result_msg_or_id, str) else "Ошибка логирования снятия. Проверьте логи."
        logger.error(
            f"Ошибка логирования /withdraw для user {user.id} (asset: {asset}, amount: {amount_str}, source: {source_name_input}): {error_to_show}")
        await update.message.reply_text(f"❌ {error_to_show}", parse_mode=ParseMode.HTML)


@admin_only
async def transfer_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    command_text = update.message.text
    logger.info(
        f"Command '/transfer' (текст: '{command_text}') от user {user.id} ({user.username or 'N/A'}). Args: {context.args}")

    args = list(context.args)
    num_positional = 4
    positional_args, named_args = parse_command_args_advanced(
        args, num_positional)

    if len(positional_args) < num_positional:
        await update.message.reply_text("Ошибка: <code>/transfer ASSET QTY FROM TO [ключи...]</code>", parse_mode=ParseMode.HTML)
        return

    try:
        asset_input, amount_str, source_name_input, destination_name_input = positional_args
        asset = asset_input.upper()
        source_entity_type = _determine_entity_type(
            source_name_input, "INTERNAL_ACCOUNT")
        destination_entity_type = _determine_entity_type(
            destination_name_input, "INTERNAL_ACCOUNT")

        if source_entity_type == "EXTERNAL" or destination_entity_type == "EXTERNAL":
            logger.warning(
                f"Попытка /transfer с внешним счетом от user {user.id}. From: {source_name_input}({source_entity_type}), To: {destination_name_input}({destination_entity_type})")
            await update.message.reply_text("❌ Ошибка: Для /transfer оба счета (FROM и TO) должны быть внутренними (из списков KNOWN_EXCHANGES или KNOWN_WALLETS в config.py).", parse_mode=ParseMode.HTML)
            return

        notes, tx_id = named_args.get("notes"), named_args.get("tx_id")
        fee_amount_str, fee_asset_str = named_args.get(
            'fee'), named_args.get('fee_asset')

        try:
            movement_timestamp = utils.parse_datetime_from_args(named_args)
            if Decimal(amount_str.replace(',', '.')) <= Decimal('0'):
                raise ValueError("Количество должно быть больше 0.")
        except (InvalidOperation, ValueError) as e:
            await update.message.reply_text(f"❌ Ошибка в данных: {e}", parse_mode=ParseMode.HTML)
            return

        logger.info(
            f"Обработка /transfer для user {user.id}: asset='{asset}', amount='{amount_str}', src_name='{source_name_input}' (type: {source_entity_type}), dest_name='{destination_name_input}' (type: {destination_entity_type}), named_args='{named_args}'")

        success, result_msg_or_id = log_fund_movement("TRANSFER", asset, amount_str, source_entity_type, source_name_input,
                                                      destination_entity_type, destination_name_input,
                                                      fee_amount_str=fee_amount_str, fee_asset=fee_asset_str,
                                                      transaction_id_blockchain=tx_id, notes=notes, movement_timestamp_obj=movement_timestamp)
        if success:
            await update.message.reply_text(f"✅ Перевод {amount_str} {asset} с '{source_name_input}' на '{destination_name_input}' залогирован. ID: <code>{result_msg_or_id}</code>", parse_mode=ParseMode.HTML)
        else:
            error_to_show = result_msg_or_id if isinstance(
                result_msg_or_id, str) else "Ошибка логирования перевода. Проверьте логи."
            logger.error(
                f"Ошибка логирования /transfer для user {user.id} (asset: {asset}, amount: {amount_str}, from: {source_name_input}, to: {destination_name_input}): {error_to_show}")
            await update.message.reply_text(f"❌ {error_to_show}", parse_mode=ParseMode.HTML)

    except (InvalidOperation, ValueError) as e_val:
        await update.message.reply_text(f"❌ Ошибка данных: {e_val}", parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(
            f"Критическая ошибка в /transfer от user {user.id} (текст: '{command_text}'). Ошибка: {e}", exc_info=True)
        await update.message.reply_text(f"Внутренняя ошибка при обработке /transfer: {e}")


@admin_only
async def portfolio_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    command_text = update.message.text
    logger.info(
        f"Command '/portfolio' (текст: '{command_text}') от user {user.id} ({user.username or 'N/A'}).")
    try:
        positions = sheets_service.get_all_open_positions()
        if positions is None:
            logger.error(
                f"Ошибка получения открытых позиций для /portfolio (user: {user.id}). sheets_service.get_all_open_positions вернул None.")
            await update.message.reply_text("❌ Не удалось загрузить открытые позиции. Проверьте логи сервера.")
            return
        if not positions:
            await update.message.reply_text("Нет открытых позиций.")
            return

        reply_text = "<u><b>💼 Открытые Позиции:</b></u>\n"
        for pos in positions:
            try:
                net_amount = Decimal(str(pos.get('Net_Amount', '0')).replace(
                    ',', '.')).quantize(Decimal(config.QTY_DISPLAY_PRECISION))
                avg_price = Decimal(str(pos.get('Avg_Entry_Price', '0')).replace(
                    ',', '.')).quantize(Decimal(config.PRICE_DISPLAY_PRECISION))
                curr_price_str = pos.get('Current_Price')
                curr_price = Decimal(str(curr_price_str).replace(',', '.')).quantize(Decimal(config.PRICE_DISPLAY_PRECISION)) if curr_price_str and str(
                    curr_price_str).strip() and str(curr_price_str).lower() != 'n/a' else "N/A"
                unreal_pnl_str = pos.get('Unrealized_PNL')
                unreal_pnl = Decimal(str(unreal_pnl_str).replace(',', '.')).quantize(Decimal(config.USD_DISPLAY_PRECISION)) if unreal_pnl_str and str(
                    unreal_pnl_str).strip() and str(unreal_pnl_str).lower() != 'n/a' else "N/A"
                pnl_sign = "+" if isinstance(unreal_pnl,
                                             Decimal) and unreal_pnl > 0 else ""
            except (InvalidOperation, TypeError) as e_format:
                logger.warning(
                    f"Ошибка форматирования данных позиции в /portfolio для user {user.id}: {pos}. Ошибка: {e_format}")
                net_amount, avg_price, curr_price, unreal_pnl, pnl_sign = [
                    "N/A"]*5
            reply_text += (f"<b>{pos.get('Symbol')}</b> ({pos.get('Exchange','N/A')})\n"
                           f"  Кол-во: {net_amount} | Ср.вход: {avg_price}\n"
                           f"  Текущая: {curr_price} | Нереал.PNL: {pnl_sign}{unreal_pnl}\n\n")
        if len(reply_text) > 4090:
            reply_text = reply_text[:4085] + "\n..."
        await update.message.reply_text(reply_text, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(
            f"Критическая ошибка в /portfolio от user {user.id}. Ошибка: {e}", exc_info=True)
        await update.message.reply_text("❌ Произошла ошибка при получении портфеля.")


@admin_only
async def history_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    command_text = update.message.text
    logger.info(
        f"Command '/history' (текст: '{command_text}') от user {user.id} ({user.username or 'N/A'}). Args: {context.args}")

    args = list(context.args)
    if not args:
        await update.message.reply_text("Использование: <code>/history SYMBOL</code>", parse_mode=ParseMode.HTML)
        return
    symbol = args[0].upper()

    try:
        trades_all = sheets_service.get_all_core_trades()
        if trades_all is None:
            logger.error(
                f"Ошибка получения истории сделок для /history (user: {user.id}, symbol: {symbol}). sheets_service.get_all_core_trades вернул None.")
            await update.message.reply_text(f"❌ Не удалось загрузить историю сделок для {symbol}. Проверьте логи сервера.")
            return

        trades = [t for t in trades_all if str(
            t.get('Symbol', '')).upper() == symbol]
        if not trades:
            await update.message.reply_text(f"Нет истории сделок для {symbol}.")
            return

        reply_text = f"<u><b>📜 История сделок для {symbol} (макс. последние 10):</b></u>\n"
        def get_datetime_from_trade(trade_item): ts_str = trade_item.get('Timestamp'); return datetime.strptime(
            ts_str, "%Y-%m-%d %H:%M:%S") if ts_str and isinstance(ts_str, str) else datetime.min
        sorted_trades = sorted(
            trades, key=get_datetime_from_trade, reverse=True)
        for trade in sorted_trades[:10]:
            try:
                amount = Decimal(str(trade.get('Amount', '0')).replace(
                    ',', '.')).quantize(Decimal(config.QTY_DISPLAY_PRECISION))
                price = Decimal(str(trade.get('Price', '0')).replace(',', '.')).quantize(
                    Decimal(config.PRICE_DISPLAY_PRECISION))
                pnl_str = trade.get('Trade_PNL')
                pnl_display = ""
                if pnl_str and str(pnl_str).strip() and str(pnl_str).lower() != 'n/a':
                    try:
                        pnl_val = Decimal(str(pnl_str).replace(',', '.')).quantize(
                            Decimal(config.USD_DISPLAY_PRECISION))
                        pnl_sign = "+" if pnl_val > 0 else ""
                        pnl_display = f"PNL: {pnl_sign}{pnl_val}"
                    except InvalidOperation:
                        pnl_display = f"PNL: {pnl_str}"
            except (InvalidOperation, TypeError, AttributeError) as e_format:
                logger.warning(
                    f"Ошибка форматирования сделки в /history для user {user.id}, symbol {symbol}: {trade}. Ошибка: {e_format}")
                amount, price, pnl_display = "N/A", "N/A", ""
            reply_text += (f"<pre>{trade.get('Timestamp')} {str(trade.get('Type','')).upper():<4} {str(amount):<12} {symbol} @ {str(price):<15} ({str(trade.get('Exchange','N/A'))}) {pnl_display}</pre>\n")
        if len(reply_text) > 4090:
            reply_text = reply_text[:4085] + "\n..."
        await update.message.reply_text(reply_text, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(
            f"Критическая ошибка в /history для user {user.id}, symbol {symbol}. Ошибка: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Произошла ошибка при получении истории для {symbol}.")


@admin_only
async def average_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    command_text = update.message.text
    logger.info(
        f"Command '/average' (текст: '{command_text}') от user {user.id} ({user.username or 'N/A'}). Args: {context.args}")

    args = list(context.args)
    positional_args, named_args = parse_command_args_advanced(args, 1)

    if not positional_args:
        logger.warning(
            f"Команда /average вызвана без символа от user {user.id}.")
        await update.message.reply_text("Использование: <code>/average SYMBOL [exch:EXCH]</code>", parse_mode=ParseMode.HTML)
        return

    symbol = positional_args[0].upper()
    exchange_name = named_args.get('exchange', named_args.get('exch'))

    try:
        row_num, position_data = sheets_service.find_position_by_symbol(
            symbol, exchange_name)
        if position_data is None:
            await update.message.reply_text(f"Нет открытой позиции для {symbol}" + (f" на '{exchange_name}'." if exchange_name else "."))
            return

        try:
            net_amount = Decimal(str(position_data.get('Net_Amount', '0')).replace(
                ',', '.')).quantize(Decimal(config.QTY_DISPLAY_PRECISION))
            avg_price = Decimal(str(position_data.get('Avg_Entry_Price', '0')).replace(
                ',', '.')).quantize(Decimal(config.PRICE_DISPLAY_PRECISION))
        except (InvalidOperation, TypeError) as e_format:
            logger.warning(
                f"Ошибка форматирования данных позиции в /average для user {user.id}, symbol {symbol}: {position_data}. Ошибка: {e_format}")
            net_amount, avg_price = "N/A", "N/A"

        reply_text = (f"<u><b>📊 Средняя цена для {symbol}" + (f" на {exchange_name}" if exchange_name else "") + ":</b></u>\n"
                      f"  Общее кол-во: <code>{net_amount}</code>\n"
                      f"  Средняя цена входа: <code>{avg_price}</code>\n")
        await update.message.reply_text(reply_text, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(
            f"Критическая ошибка в /average для user {user.id}, symbol {symbol}. Ошибка: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Произошла ошибка при расчете средней цены для {symbol}.")


@admin_only
async def movements_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    command_text = update.message.text
    logger.info(
        f"Command '/movements' (текст: '{command_text}') от user {user.id} ({user.username or 'N/A'}).")
    try:
        movements = sheets_service.get_all_fund_movements()
        if movements is None:
            logger.error(
                f"Ошибка получения движений средств для /movements (user: {user.id}). sheets_service.get_all_fund_movements вернул None.")
            await update.message.reply_text("❌ Не удалось загрузить историю движения средств. Проверьте логи сервера.")
            return
        if not movements:
            await update.message.reply_text("Нет записей о движении средств.")
            return

        reply_text = "<u><b>Детальное Движение Средств (макс 10):</b></u>\n"
        def get_movement_datetime(item): ts = item.get('Timestamp'); return datetime.strptime(
            ts, "%Y-%m-%d %H:%M:%S") if ts else datetime.min
        sorted_movements = sorted(
            movements, key=get_movement_datetime, reverse=True)
        for move in sorted_movements[:10]:
            try:
                amount_str = move.get('Amount', '0')
                asset_display = move.get('Asset', '')
                is_stable = asset_display.upper() in [
                    'USD', 'EUR', 'USDT', 'USDC', 'DAI', 'BUSD', config.DEFAULT_DEPOSIT_WITHDRAW_ASSET.upper()]
                prec_str = config.USD_DISPLAY_PRECISION if is_stable else config.QTY_DISPLAY_PRECISION
                amount_dec = Decimal(str(amount_str).replace(
                    ',', '.')).quantize(Decimal(prec_str))
            except (InvalidOperation, TypeError) as e_format:
                logger.warning(
                    f"Ошибка форматирования суммы движения в /movements для user {user.id}: {move}. Ошибка: {e_format}")
                amount_dec = "N/A"
            reply_text += (f"{move.get('Timestamp')} - <b>{move.get('Type')} {amount_dec} {move.get('Asset')}</b>\n"
                           f"  <pre>Из: {move.get('Source_Name')} ({move.get('Source_Entity_Type')})\n"
                           f"  В:  {move.get('Destination_Name')} ({move.get('Destination_Entity_Type')})</pre>\n")
            fee_amount_str = move.get('Fee_Amount')
            if fee_amount_str and str(fee_amount_str).strip() and Decimal(str(fee_amount_str).replace(',', '.')) > Decimal('0'):
                try:
                    fee_asset = move.get('Fee_Asset', '')
                    is_stable_fee = fee_asset.upper(
                    ) in ['USD', 'EUR', 'USDT', 'USDC', 'DAI', 'BUSD']
                    fee_prec_str = config.USD_DISPLAY_PRECISION if is_stable_fee else config.QTY_DISPLAY_PRECISION
                    fee_dec = Decimal(str(fee_amount_str).replace(
                        ',', '.')).quantize(Decimal(fee_prec_str))
                    reply_text += f"  Комиссия: {fee_dec} {fee_asset}\n"
                except (InvalidOperation, TypeError):
                    reply_text += f"  Комиссия: {fee_amount_str} {move.get('Fee_Asset','')}\n"
            if move.get('Notes'):
                reply_text += f"  Заметка: <i>{move.get('Notes')}</i>\n"
            reply_text += "\n"
        if len(reply_text) > 4090:
            reply_text = reply_text[:4085] + "\n..."
        await update.message.reply_text(reply_text, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(
            f"Критическая ошибка в /movements от user {user.id}. Ошибка: {e}", exc_info=True)
        await update.message.reply_text("❌ Произошла ошибка при получении истории движения средств.")


@admin_only
async def updater_status_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    command_text = update.message.text
    logger.info(
        f"Command '/updater_status' (текст: '{command_text}') от user {user.id} ({user.username or 'N/A'}).")
    try:
        last_run_cell = config.UPDATER_LAST_RUN_CELL
        sheet_name = config.SYSTEM_STATUS_SHEET_NAME
        last_run_time_str = sheets_service.read_cell_from_sheet(
            sheet_name, last_run_cell)

        if last_run_time_str is None and sheets_service.get_sheet_by_name(sheet_name) is None:
            await update.message.reply_text(f"🟡 Price Updater: Лист статуса '{sheet_name}' не найден.")
            return

        status_str = None
        status_cell_config_name = getattr(config, 'UPDATER_STATUS_CELL', None)
        if status_cell_config_name:
            status_str = sheets_service.read_cell_from_sheet(
                sheet_name, status_cell_config_name)
        elif last_run_cell:
            try:
                row, col = gspread.utils.a1_to_rowcol(last_run_cell)
                status_cell_address = gspread.utils.rowcol_to_a1(row, col + 1)
                status_str = sheets_service.read_cell_from_sheet(
                    sheet_name, status_cell_address)
            except Exception as e_cell:
                logger.debug(
                    f"Не удалось прочитать статус Price Updater из соседней ячейки '{status_cell_address}': {e_cell}")

        reply_msg = f"🟢 Price Updater: посл. обновление в <b>{last_run_time_str}</b>" if last_run_time_str else "🟡 Price Updater: нет данных о времени последнего обновления."
        if status_str and last_run_time_str:
            reply_msg += f", статус: <b>{status_str}</b>."
        elif last_run_time_str:
            reply_msg += "."

        await update.message.reply_text(reply_msg, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(
            f"Критическая ошибка в /updater_status от user {user.id}. Ошибка: {e}", exc_info=True)
        await update.message.reply_text("🔴 Ошибка получения статуса Price Updater.")


@admin_only
async def update_analytics_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    command_text = update.message.text
    logger.info(
        f"Command '/update_analytics' (текст: '{command_text}') от user {user.id} ({user.username or 'N/A'}).")
    try:
        from analytics_service import calculate_and_update_analytics_sheet

        await update.message.reply_text("⚙️ Запускаю полное обновление аналитики...", parse_mode=ParseMode.HTML)

        logger.info(
            f"Вызов calculate_and_update_analytics_sheet для user {user.id}")
        success, message = calculate_and_update_analytics_sheet(
            triggered_by_context=f"user {user.id}")

        if success:
            logger.info(
                f"/update_analytics успешно завершено для user {user.id}. Сообщение: {message}")
            await update.message.reply_text(f"✅ Обновление аналитики завершено!\n{message}", parse_mode=ParseMode.HTML)
        else:
            logger.error(
                f"Ошибка выполнения /update_analytics для user {user.id}. Сообщение от сервиса: {message}")
            await update.message.reply_text(f"❌ Ошибка обновления аналитики:\n{message}", parse_mode=ParseMode.HTML)

    except ImportError:
        logger.critical(
            "Критическая ошибка: Модуль analytics_service не найден.", exc_info=True)
        await update.message.reply_text("❌ Критическая Ошибка: Модуль аналитики не найден. Обратитесь к администратору.", parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(
            f"Критическая ошибка в /update_analytics от user {user.id}. Ошибка: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Произошла непредвиденная ошибка при обновлении аналитики: {e}", parse_mode=ParseMode.HTML)
