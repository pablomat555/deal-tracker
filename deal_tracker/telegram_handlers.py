# deal_tracker/telegram_handlers.py
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
import re
import gspread

from telegram import Update
from telegram.ext import CallbackContext
from telegram.constants import ParseMode

import config
from trade_logger import log_trade, log_fund_movement
import sheets_service

logger = logging.getLogger(__name__)


# ++ НОВЫЙ, НАДЕЖНЫЙ ПАРСЕР КОМАНД ++
def parse_command_args(args: list[str]) -> tuple[list[str], dict[str, str]]:
    """
    Разбирает список аргументов команды на позиционные и именованные.
    - Позиционные аргументы идут до первого именованного.
    - Именованные аргументы имеют формат `ключ:значение` или `ключ:'значение в кавычках'`.
    """
    positional_args = []
    named_args = {}
    is_positional = True

    # Сначала объединяем аргументы в кавычках
    processed_args = []
    in_quote = False
    buffer = ""
    quote_char = ''
    for arg in " ".join(args).split():
        if not in_quote and (arg.startswith("notes:'") or arg.startswith('notes:"')):
            in_quote = True
            quote_char = arg[6]
            buffer = arg
            if arg.endswith(quote_char) and len(arg) > 7:
                processed_args.append(buffer)
                buffer = ""
                in_quote = False
            continue
        if in_quote:
            buffer += " " + arg
            if arg.endswith(quote_char):
                processed_args.append(buffer)
                buffer = ""
                in_quote = False
            continue
        processed_args.append(arg)

    for arg in processed_args:
        # Проверяем, является ли аргумент именованным
        match = re.match(r"([a-zA-Z_а-яА-Я]+):(.*)", arg)
        if match and is_positional:
            is_positional = False

        if is_positional:
            positional_args.append(arg)
        else:
            if ':' in arg:
                key, value = arg.split(':', 1)
                # Удаляем кавычки, если они есть
                if (value.startswith("'") and value.endswith("'")) or \
                   (value.startswith('"') and value.endswith('"')):
                    value = value[1:-1]
                named_args[key.lower().strip()] = value.strip()
            else:
                logger.warning(
                    f"Именованный аргумент без ключа: '{arg}'. Игнорируется.")

    return positional_args, named_args


# ++ НОВАЯ, НАДЕЖНАЯ ФУНКЦИЯ ПАРСИНГА ДАТЫ ++
def _parse_user_date(named_args: dict) -> datetime | None:
    """
    Извлекает и парсит дату из именованных аргументов.
    Возвращает None, если дата не указана.
    """
    date_str = named_args.get('date')
    if not date_str:
        return None

    # Формат 'ГГГГММДД'
    if re.fullmatch(r'\d{8}', date_str):
        try:
            # Возвращаем naive datetime, таймзону добавит логгер
            return datetime.strptime(date_str, '%Y%m%d')
        except ValueError as e:
            logger.error(f"Ошибка парсинга даты '{date_str}': {e}")
            raise ValueError(
                f"Неверный формат даты '{date_str}'. Ожидается ГГГГММДД.")

    # Другие форматы
    formats_to_try = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]
    for fmt in formats_to_try:
        try:
            # Возвращаем naive datetime
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    logger.error(f"Не удалось распознать формат даты: '{date_str}'")
    raise ValueError(
        f"Неизвестный формат даты: '{date_str}'. Используйте ГГГГ-ММ-ДД или ГГГГММДД.")


def _determine_entity_type(name: str, default_type_if_unknown="EXTERNAL") -> str:
    """
    Определяет тип сущности (биржа, кошелек, внешняя) по имени.
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


def admin_only(func):
    """Декоратор для ограничения доступа к командам только для админов."""
    async def wrapped(update: Update, context: CallbackContext, *args, **kwargs):
        user = update.effective_user
        user_id_str = str(user.id)
        admin_ids_str = getattr(config, 'TELEGRAM_ADMIN_IDS_STR', getattr(
            config, 'TELEGRAM_CHAT_ID', ''))
        admin_ids = [s.strip() for s in admin_ids_str.split(',') if s.strip()]

        if user_id_str not in admin_ids:
            logger.warning(
                f"Неавторизованный доступ к '{func.__name__}' от user ID {user_id_str}.")
            await update.message.reply_text("⛔️ У вас нет прав для выполнения этой команды.")
            return
        return await func(update, context, *args, **kwargs)
    return wrapped


async def start_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    logger.info(
        f"Command '/start' от user {user.id} ({user.username or 'N/A'}).")
    user_name = user.first_name
    help_text = (
        f"Привет, {user_name}!\n"
        "Я бот для учета ваших криптовалютных сделок и финансов.\n\n"
        "<b>Доступные команды:</b>\n"
        "/help - Показать это сообщение\n"
        "--- <u>Торговля</u> ---\n"
        "<code>/buy SYMBOL QTY PRICE exch:NAME [ключи...]</code>\n"
        "<code>/sell SYMBOL QTY PRICE exch:NAME [ключи...]</code>\n"
        "  <i>Опц. ключи: notes, id, date, fee, fee_asset</i>\n"
        "  <i>Форматы date: <code>ГГГГ-ММ-ДД ЧЧ:ММ</code>, <code>ГГГГММДД</code></i>\n"
        "--- <u>Финансы</u> ---\n"
        "<code>/deposit ASSET AMOUNT dest_name:NAME [ключи...]</code>\n"
        "<code>/withdraw ASSET AMOUNT source_name:NAME [ключи...]</code>\n"
        "<code>/transfer ASSET QTY FROM TO [ключи...]</code>\n"
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
    await start_command(update, context)


@admin_only
async def buy_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    logger.info(f"Command '/buy' от user {user.id}: {update.message.text}")
    try:
        positional, named = parse_command_args(context.args)

        if len(positional) < 3:
            await update.message.reply_text("Ошибка: <code>/buy SYMBOL QTY PRICE exch:NAME [date:...]</code>", parse_mode=ParseMode.HTML)
            return

        symbol, qty_str, price_str = positional

        exchange_name = named.get('exch')
        if not exchange_name:
            await update.message.reply_text("❌ Ошибка: Укажите биржу через `exch:ИМЯ_БИРЖИ`.", parse_mode=ParseMode.HTML)
            return

        # Валидация числовых данных
        try:
            if Decimal(qty_str.replace(',', '.')) <= 0 or Decimal(price_str.replace(',', '.')) <= 0:
                raise ValueError("Количество и цена должны быть больше нуля.")
        except (InvalidOperation, ValueError) as e:
            await update.message.reply_text(f"❌ Ошибка в числовых данных: {e}")
            return

        trade_timestamp = _parse_user_date(named)

        success, result_msg_or_id = log_trade(
            trade_type="BUY",
            symbol=symbol,
            qty_str=qty_str,
            price_str=price_str,
            exchange_name=exchange_name,
            named_args=named,
            trade_timestamp_obj=trade_timestamp
        )

        if success:
            await update.message.reply_text(f"✅ Покупка {qty_str} {symbol} @ {price_str} залогирована. ID: <code>{result_msg_or_id}</code>", parse_mode=ParseMode.HTML)
        else:
            await update.message.reply_text(f"❌ {result_msg_or_id}", parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(f"Критическая ошибка в /buy: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Внутренняя ошибка: {e}")


@admin_only
async def sell_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    logger.info(f"Command '/sell' от user {user.id}: {update.message.text}")
    try:
        positional, named = parse_command_args(context.args)

        if len(positional) < 3:
            await update.message.reply_text("Ошибка: <code>/sell SYMBOL QTY PRICE exch:NAME [date:...]</code>", parse_mode=ParseMode.HTML)
            return

        symbol, qty_str, price_str = positional

        exchange_name = named.get('exch')
        if not exchange_name:
            await update.message.reply_text("❌ Ошибка: Укажите биржу через `exch:ИМЯ_БИРЖИ`.", parse_mode=ParseMode.HTML)
            return

        try:
            if Decimal(qty_str.replace(',', '.')) <= 0 or Decimal(price_str.replace(',', '.')) <= 0:
                raise ValueError("Количество и цена должны быть больше нуля.")
        except (InvalidOperation, ValueError) as e:
            await update.message.reply_text(f"❌ Ошибка в числовых данных: {e}")
            return

        trade_timestamp = _parse_user_date(named)

        success, result_msg_or_id = log_trade(
            trade_type="SELL",
            symbol=symbol,
            qty_str=qty_str,
            price_str=price_str,
            exchange_name=exchange_name,
            named_args=named,
            trade_timestamp_obj=trade_timestamp
        )

        if success:
            await update.message.reply_text(f"✅ Продажа {qty_str} {symbol} @ {price_str} залогирована. ID: <code>{result_msg_or_id}</code>", parse_mode=ParseMode.HTML)
        else:
            await update.message.reply_text(f"❌ {result_msg_or_id}", parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(f"Критическая ошибка в /sell: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Внутренняя ошибка: {e}")


@admin_only
async def deposit_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    logger.info(f"Command '/deposit' от user {user.id}: {update.message.text}")
    try:
        positional, named = parse_command_args(context.args)

        if len(positional) < 2:
            await update.message.reply_text("Использование: <code>/deposit ASSET AMOUNT dest_name:NAME [ключи...]</code>", parse_mode=ParseMode.HTML)
            return

        asset, amount_str = positional
        destination_name = named.get('dest_name')

        if not destination_name:
            await update.message.reply_text("❌ Ошибка: Укажите счет назначения через `dest_name:ИМЯ_СЧЕТА`.", parse_mode=ParseMode.HTML)
            return

        source_entity_type = "EXTERNAL"
        source_name = getattr(
            config, 'DEFAULT_DEPOSIT_SOURCE_NAME', "External Inflow")
        destination_entity_type = _determine_entity_type(
            destination_name, "INTERNAL_ACCOUNT")

        notes, tx_id = named.get("notes"), named.get("tx_id")
        fee_amount_str, fee_asset_str = named.get(
            'fee'), named.get('fee_asset')

        try:
            movement_timestamp = _parse_user_date(named)
            if Decimal(amount_str.replace(',', '.')) <= Decimal('0'):
                raise ValueError("Сумма должна быть больше 0.")
        except (InvalidOperation, ValueError) as e:
            await update.message.reply_text(f"❌ Ошибка в данных: {e}", parse_mode=ParseMode.HTML)
            return

        success, result_msg_or_id = log_fund_movement(
            movement_type="DEPOSIT", asset=asset.upper(), amount_str=amount_str,
            source_entity_type=source_entity_type, source_name=source_name,
            destination_entity_type=destination_entity_type, destination_name=destination_name,
            fee_amount_str=fee_amount_str, fee_asset=fee_asset_str,
            transaction_id_blockchain=tx_id, notes=notes, movement_timestamp_obj=movement_timestamp)

        if success:
            await update.message.reply_text(f"✅ Депозит {amount_str} {asset.upper()} на '{destination_name}' залогирован. ID: <code>{result_msg_or_id}</code>", parse_mode=ParseMode.HTML)
        else:
            await update.message.reply_text(f"❌ {result_msg_or_id}", parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Критическая ошибка в /deposit: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Внутренняя ошибка: {e}")


@admin_only
async def withdraw_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    logger.info(
        f"Command '/withdraw' от user {user.id}: {update.message.text}")
    try:
        positional, named = parse_command_args(context.args)

        if len(positional) < 2:
            await update.message.reply_text("Использование: <code>/withdraw ASSET AMOUNT source_name:NAME [ключи...]</code>", parse_mode=ParseMode.HTML)
            return

        asset, amount_str = positional
        source_name = named.get('source_name')

        if not source_name:
            await update.message.reply_text("❌ Ошибка: Укажите счет списания через `source_name:ИМЯ_СЧЕТА`.", parse_mode=ParseMode.HTML)
            return

        source_entity_type = _determine_entity_type(
            source_name, "INTERNAL_ACCOUNT")
        destination_entity_type = "EXTERNAL"
        destination_name = getattr(
            config, 'DEFAULT_WITHDRAW_DESTINATION_NAME', "External Outflow")

        notes, tx_id = named.get("notes"), named.get("tx_id")
        fee_amount_str, fee_asset_str = named.get(
            'fee'), named.get('fee_asset')

        try:
            movement_timestamp = _parse_user_date(named)
            if Decimal(amount_str.replace(',', '.')) <= Decimal('0'):
                raise ValueError("Сумма должна быть больше 0.")
        except (InvalidOperation, ValueError) as e:
            await update.message.reply_text(f"❌ Ошибка в данных: {e}", parse_mode=ParseMode.HTML)
            return

        success, result_msg_or_id = log_fund_movement(
            movement_type="WITHDRAWAL", asset=asset.upper(), amount_str=amount_str,
            source_entity_type=source_entity_type, source_name=source_name,
            destination_entity_type=destination_entity_type, destination_name=destination_name,
            fee_amount_str=fee_amount_str, fee_asset=fee_asset_str,
            transaction_id_blockchain=tx_id, notes=notes, movement_timestamp_obj=movement_timestamp)

        if success:
            await update.message.reply_text(f"✅ Снятие {amount_str} {asset.upper()} с '{source_name}' залогировано. ID: <code>{result_msg_or_id}</code>", parse_mode=ParseMode.HTML)
        else:
            await update.message.reply_text(f"❌ {result_msg_or_id}", parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Критическая ошибка в /withdraw: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Внутренняя ошибка: {e}")


@admin_only
async def transfer_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    logger.info(
        f"Command '/transfer' от user {user.id}: {update.message.text}")
    try:
        positional, named = parse_command_args(context.args)

        if len(positional) < 4:
            await update.message.reply_text("Ошибка: <code>/transfer ASSET QTY FROM TO [ключи...]</code>", parse_mode=ParseMode.HTML)
            return

        asset, amount_str, source_name, destination_name = positional

        source_entity_type = _determine_entity_type(
            source_name, "INTERNAL_ACCOUNT")
        destination_entity_type = _determine_entity_type(
            destination_name, "INTERNAL_ACCOUNT")

        if source_entity_type == "EXTERNAL" or destination_entity_type == "EXTERNAL":
            await update.message.reply_text("❌ Ошибка: Для /transfer оба счета должны быть внутренними.", parse_mode=ParseMode.HTML)
            return

        notes, tx_id = named.get("notes"), named.get("tx_id")
        fee_amount_str, fee_asset_str = named.get(
            'fee'), named.get('fee_asset')

        try:
            movement_timestamp = _parse_user_date(named)
            if Decimal(amount_str.replace(',', '.')) <= Decimal('0'):
                raise ValueError("Количество должно быть больше 0.")
        except (InvalidOperation, ValueError) as e:
            await update.message.reply_text(f"❌ Ошибка в данных: {e}", parse_mode=ParseMode.HTML)
            return

        success, result_msg_or_id = log_fund_movement(
            "TRANSFER", asset.upper(), amount_str, source_entity_type, source_name,
            destination_entity_type, destination_name,
            fee_amount_str=fee_amount_str, fee_asset=fee_asset_str,
            transaction_id_blockchain=tx_id, notes=notes, movement_timestamp_obj=movement_timestamp)

        if success:
            await update.message.reply_text(f"✅ Перевод {amount_str} {asset.upper()} с '{source_name}' на '{destination_name}' залогирован. ID: <code>{result_msg_or_id}</code>", parse_mode=ParseMode.HTML)
        else:
            await update.message.reply_text(f"❌ {result_msg_or_id}", parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(f"Критическая ошибка в /transfer: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Внутренняя ошибка: {e}")


@admin_only
async def portfolio_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    logger.info(f"Command '/portfolio' от user {user.id}.")
    try:
        positions = sheets_service.get_all_open_positions()
        if positions is None:
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
                    f"Ошибка форматирования позиции в /portfolio: {pos}. Ошибка: {e_format}")
                net_amount, avg_price, curr_price, unreal_pnl, pnl_sign = [
                    "N/A"]*5
            reply_text += (f"<b>{pos.get('Symbol')}</b> ({pos.get('Exchange','N/A')})\n"
                           f"  Кол-во: {net_amount} | Ср.вход: {avg_price}\n"
                           f"  Текущая: {curr_price} | Нереал.PNL: {pnl_sign}{unreal_pnl}\n\n")
        if len(reply_text) > 4090:
            reply_text = reply_text[:4085] + "\n..."
        await update.message.reply_text(reply_text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Критическая ошибка в /portfolio: {e}", exc_info=True)
        await update.message.reply_text("❌ Произошла ошибка при получении портфеля.")


@admin_only
async def history_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    logger.info(f"Command '/history' от user {user.id}.")
    if not context.args:
        await update.message.reply_text("Использование: <code>/history SYMBOL</code>", parse_mode=ParseMode.HTML)
        return
    symbol = context.args[0].upper()
    try:
        trades_all = sheets_service.get_all_core_trades()
        if trades_all is None:
            await update.message.reply_text(f"❌ Не удалось загрузить историю сделок для {symbol}.")
            return
        trades = [t for t in trades_all if str(
            t.get('Symbol', '')).upper() == symbol]
        if not trades:
            await update.message.reply_text(f"Нет истории сделок для {symbol}.")
            return
        reply_text = f"<u><b>📜 История сделок для {symbol} (макс. последние 10):</b></u>\n"

        def get_datetime_from_trade(trade_item):
            ts_str = trade_item.get('Timestamp')
            return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S") if ts_str and isinstance(ts_str, str) else datetime.min
        sorted_trades = sorted(
            trades, key=get_datetime_from_trade, reverse=True)
        for trade in sorted_trades[:10]:
            try:
                amount = Decimal(str(trade.get('Amount', '0')).replace(
                    ',', '.')).quantize(Decimal(config.QTY_DISPLAY_PRECISION))
                price = Decimal(str(trade.get('Price', '0')).replace(',', '.')).quantize(
                    Decimal(config.PRICE_DISPLAY_PRECISION))
                pnl_display = ""
                pnl_str = trade.get('Trade_PNL')
                if pnl_str and str(pnl_str).strip() and str(pnl_str).lower() != 'n/a':
                    pnl_val = Decimal(str(pnl_str).replace(',', '.')).quantize(
                        Decimal(config.USD_DISPLAY_PRECISION))
                    pnl_sign = "+" if pnl_val > 0 else ""
                    pnl_display = f"PNL: {pnl_sign}{pnl_val}"
            except (InvalidOperation, TypeError, AttributeError) as e_format:
                logger.warning(
                    f"Ошибка форматирования сделки в /history: {trade}. Ошибка: {e_format}")
                amount, price, pnl_display = "N/A", "N/A", ""
            reply_text += (f"<pre>{trade.get('Timestamp')} {str(trade.get('Type','')).upper():<4} {str(amount):<12} {symbol} @ {str(price):<15} ({str(trade.get('Exchange','N/A'))}) {pnl_display}</pre>\n")
        if len(reply_text) > 4090:
            reply_text = reply_text[:4085] + "\n..."
        await update.message.reply_text(reply_text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Критическая ошибка в /history: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Произошла ошибка при получении истории для {symbol}.")


@admin_only
async def average_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    logger.info(f"Command '/average' от user {user.id}.")
    positional, named = parse_command_args(context.args)
    if not positional:
        await update.message.reply_text("Использование: <code>/average SYMBOL [exch:EXCH]</code>", parse_mode=ParseMode.HTML)
        return
    symbol = positional[0].upper()
    exchange_name = named.get('exchange', named.get('exch'))
    try:
        row_num, position_data = sheets_service.find_position_by_symbol(
            symbol, exchange_name)
        if position_data is None:
            await update.message.reply_text(f"Нет открытой позиции для {symbol}" + (f" на '{exchange_name}'." if exchange_name else "."))
            return
        net_amount = Decimal(str(position_data.get('Net_Amount', '0')).replace(
            ',', '.')).quantize(Decimal(config.QTY_DISPLAY_PRECISION))
        avg_price = Decimal(str(position_data.get('Avg_Entry_Price', '0')).replace(
            ',', '.')).quantize(Decimal(config.PRICE_DISPLAY_PRECISION))
        reply_text = (f"<u><b>📊 Средняя цена для {symbol}" + (f" на {exchange_name}" if exchange_name else "") + ":</b></u>\n"
                      f"  Общее кол-во: <code>{net_amount}</code>\n"
                      f"  Средняя цена входа: <code>{avg_price}</code>\n")
        await update.message.reply_text(reply_text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Критическая ошибка в /average: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Произошла ошибка при расчете средней цены для {symbol}.")


@admin_only
async def movements_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    logger.info(f"Command '/movements' от user {user.id}.")
    try:
        movements = sheets_service.get_all_fund_movements()
        if movements is None:
            await update.message.reply_text("❌ Не удалось загрузить историю движения средств.")
            return
        if not movements:
            await update.message.reply_text("Нет записей о движении средств.")
            return
        reply_text = "<u><b>Детальное Движение Средств (макс 10):</b></u>\n"

        def get_movement_datetime(item):
            ts = item.get('Timestamp')
            return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") if ts else datetime.min
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
            except (InvalidOperation, TypeError):
                amount_dec = "N/A"
            reply_text += (f"{move.get('Timestamp')} - <b>{move.get('Type')} {amount_dec} {move.get('Asset')}</b>\n"
                           f"  <pre>Из: {move.get('Source_Name')} ({move.get('Source_Entity_Type')})\n"
                           f"  В:  {move.get('Destination_Name')} ({move.get('Destination_Entity_Type')})</pre>\n")
            if move.get('Notes'):
                reply_text += f"  Заметка: <i>{move.get('Notes')}</i>\n"
            reply_text += "\n"
        if len(reply_text) > 4090:
            reply_text = reply_text[:4085] + "\n..."
        await update.message.reply_text(reply_text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Критическая ошибка в /movements: {e}", exc_info=True)
        await update.message.reply_text("❌ Произошла ошибка при получении истории движения средств.")


@admin_only
async def updater_status_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    logger.info(f"Command '/updater_status' от user {user.id}.")
    try:
        last_run_cell = config.UPDATER_LAST_RUN_CELL
        sheet_name = config.SYSTEM_STATUS_SHEET_NAME
        last_run_time_str = sheets_service.read_cell_from_sheet(
            sheet_name, last_run_cell)
        if last_run_time_str is None:
            await update.message.reply_text(f"🟡 Price Updater: нет данных о времени последнего обновления.")
            return
        reply_msg = f"🟢 Price Updater: посл. обновление в <b>{last_run_time_str}</b>."
        await update.message.reply_text(reply_msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(
            f"Критическая ошибка в /updater_status: {e}", exc_info=True)
        await update.message.reply_text("🔴 Ошибка получения статуса Price Updater.")


@admin_only
async def update_analytics_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    logger.info(f"Command '/update_analytics' от user {user.id}.")
    try:
        from analytics_service import calculate_and_update_analytics_sheet
        await update.message.reply_text("⚙️ Запускаю полное обновление аналитики...")
        success, message = calculate_and_update_analytics_sheet(
            triggered_by_context=f"user {user.id}")
        if success:
            await update.message.reply_text(f"✅ Обновление аналитики завершено!\n{message}")
        else:
            await update.message.reply_text(f"❌ Ошибка обновления аналитики:\n{message}")
    except ImportError:
        await update.message.reply_text("❌ Критическая Ошибка: Модуль аналитики не найден.")
    except Exception as e:
        logger.error(
            f"Критическая ошибка в /update_analytics: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Произошла непредвиденная ошибка: {e}")
