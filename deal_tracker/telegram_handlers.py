# deal_tracker/telegram_handlers.py
import logging
from decimal import Decimal, InvalidOperation
from typing import List, Optional
from telegram import Update
from telegram.ext import CallbackContext
from telegram.constants import ParseMode

import config
import utils
import sheets_service
import analytics_service
from trade_logger import log_trade, log_fund_movement
from telegram_parser import parse_command_args_advanced
from models import TradeData, PositionData, BalanceData # Убедимся, что импортируем модели

logger = logging.getLogger(__name__)


def merge_amount_parts(args: List[str]) -> List[str]:
    """
    Объединяет части суммы, разделенные пробелом.
    Например, ['USDT', '12', '000,50'] -> ['USDT', '12 000,50'].
    """
    merged_args = []
    skip_next = False
    for i in range(len(args)):
        if skip_next:
            skip_next = False
            continue
        if i + 1 < len(args) and args[i].isdigit() and ',' in args[i+1]:
            merged_args.append(f"{args[i]} {args[i+1]}")
            skip_next = True
        else:
            merged_args.append(args[i])
    return merged_args


def normalize_amount_string(amount_str: str) -> Optional[Decimal]:
    """
    Преобразует строку вида '12 000,50' или '10.000,99' в Decimal,
    удаляя пробелы и заменяя запятую на точку.
    """
    if not amount_str:
        return None
    try:
        # Удаляем пробелы, заменяем запятую на точку для унификации
        cleaned = amount_str.replace(' ', '').replace(',', '.')
        return Decimal(cleaned)
    except (InvalidOperation, TypeError):
        logger.warning(
            f"Не удалось преобразовать строку '{amount_str}' в Decimal.")
        return None

# --- Декоратор и вспомогательные функции (без изменений) ---
def admin_only(func):
    """Декоратор для ограничения доступа к командам только для администраторов."""
    async def wrapped(update: Update, context: CallbackContext, *args, **kwargs):
        user = update.effective_user
        admin_ids = [s.strip()
                     for s in config.TELEGRAM_ADMIN_IDS_STR.split(',') if s.strip()]
        if str(user.id) not in admin_ids:
            await update.message.reply_text("⛔️ У вас нет прав для выполнения этой команды.")
            return
        return await func(update, context, *args, **kwargs)
    return wrapped


async def start_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    help_text = (
        f"Привет, {user.first_name}!\n"
        "Я бот для учета ваших криптовалютных сделок и финансов.\n\n"
        "<b>Доступные команды:</b>\n"
        "/help - Показать это сообщение\n"
        "--- <u>Торговля</u> ---\n"
        "<code>/buy SYMBOL QTY PRICE exch:NAME [ключи...]</code>\n"
        "<code>/sell SYMBOL QTY PRICE exch:NAME [ключи...]</code>\n"
        "  <i>Опц. ключи: fee, fee_asset, notes, date, id, sl, tp1, tp2, tp3</i>\n"
        "--- <u>Финансы</u> ---\n"
        "<code>/deposit ASSET AMOUNT dest_name:NAME [ключи...]</code>\n"
        "<code>/withdraw ASSET AMOUNT source_name:NAME [ключи...]</code>\n"
        "<code>/transfer ASSET QTY FROM TO [ключи...]</code>\n"
        "  <i>Опц. ключи: date, notes, tx_id, fee, fee_asset</i>\n"
        "--- <u>Отчеты</u> ---\n"
        "/balance - Сводка по балансам\n"
        "/portfolio - Открытые позиции\n"
        "/history SYMBOL - История сделок по символу\n"
        "/average SYMBOL - Средняя цена входа по символу\n"
        "/updater_status - Статус обновления цен\n"
        "/update_analytics - Обновить аналитику и FIFO\n"
    )
    await update.message.reply_text(help_text, parse_mode=ParseMode.HTML)


async def help_command(update: Update, context: CallbackContext) -> None:
    await start_command(update, context)


@admin_only
async def trade_command(update: Update, context: CallbackContext, trade_type: str) -> None:
    """Общий обработчик для команд /buy и /sell."""
    command_name = update.message.text.split(' ')[0].lower()

    processed_args = merge_amount_parts(list(context.args))
    pos_args, named_args = parse_command_args_advanced(processed_args, 3)

    if len(pos_args) < 3:
        await update.message.reply_text(f"Ошибка: <code>{command_name} SYMBOL QTY PRICE exch:NAME [ключи...]</code>", parse_mode=ParseMode.HTML)
        return

    symbol = pos_args[0]
    amount_dec = normalize_amount_string(pos_args[1])
    price_dec = normalize_amount_string(pos_args[2])
    exchange = named_args.get('exch')

    if not all([amount_dec, price_dec, exchange]):
        await update.message.reply_text("Ошибка в данных. Проверьте кол-во, цену и `exch:ИМЯ`.", parse_mode=ParseMode.HTML)
        return

    timestamp = utils.parse_datetime_from_args(named_args)
    kwargs = {
        'notes': named_args.get('notes'), 'order_id': named_args.get('id'),
        'commission': normalize_amount_string(named_args.get('fee')),
        'commission_asset': named_args.get('fee_asset'),
        'sl': normalize_amount_string(named_args.get('sl')),
        'tp1': normalize_amount_string(named_args.get('tp1')),
        'tp2': normalize_amount_string(named_args.get('tp2')),
        'tp3': normalize_amount_string(named_args.get('tp3')),
    }
    # Убираем пустые значения
    kwargs = {k: v for k, v in kwargs.items() if v is not None}

    success, message = log_trade(
        trade_type=trade_type, exchange=exchange, symbol=symbol,
        amount=amount_dec, price=price_dec, timestamp=timestamp, **kwargs
    )
    if success:
        await update.message.reply_text(f"✅ {trade_type.capitalize()} {amount_dec} {symbol} @ {price_dec} залогирована.", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(f"❌ {message}", parse_mode=ParseMode.HTML)


async def buy_command(update: Update, context: CallbackContext) -> None:
    await trade_command(update, context, trade_type='BUY')


async def sell_command(update: Update, context: CallbackContext) -> None:
    await trade_command(update, context, trade_type='SELL')


@admin_only
async def movement_command(update: Update, context: CallbackContext, move_type: str) -> None:
    """Общий обработчик для /deposit, /withdraw, /transfer."""
    logger.info(
        f"[HANDLER] Получена команда /{move_type.lower()} с аргументами: {context.args}")

    processed_args = merge_amount_parts(list(context.args))
    logger.info(f"[HANDLER] Аргументы после обработки: {processed_args}")

    pos_args, named_args = parse_command_args_advanced(processed_args, 4)
    min_args = 2 if move_type != 'TRANSFER' else 3
    if len(pos_args) < min_args:
        await update.message.reply_text("Ошибка: недостаточно аргументов.", parse_mode=ParseMode.HTML)
        return

    asset = pos_args[0]
    amount_dec = normalize_amount_string(pos_args[1])
    if not amount_dec or amount_dec <= Decimal('0'):
        await update.message.reply_text("Ошибка: некорректная сумма.", parse_mode=ParseMode.HTML)
        return

    kwargs = {}
    if move_type == 'DEPOSIT':
        kwargs['destination_name'] = named_args.get('dest_name')
        if not kwargs['destination_name']:
            await update.message.reply_text("Ошибка: для депозита укажите `dest_name:ИМЯ`.", parse_mode=ParseMode.HTML)
            return
    elif move_type == 'WITHDRAWAL':
        kwargs['source_name'] = named_args.get('source_name')
        if not kwargs['source_name']:
            await update.message.reply_text("Ошибка: для снятия укажите `source_name:ИМЯ`.", parse_mode=ParseMode.HTML)
            return
    elif move_type == 'TRANSFER':
        if len(pos_args) < 4:
            await update.message.reply_text("Ошибка: для transfer укажите <code>/transfer ASSET QTY FROM TO</code>.", parse_mode=ParseMode.HTML)
            return
        kwargs['source_name'] = pos_args[2]
        kwargs['destination_name'] = pos_args[3]

    timestamp_obj = utils.parse_datetime_from_args(named_args)
    kwargs['fee_amount'] = normalize_amount_string(named_args.get('fee'))
    kwargs['fee_asset'] = named_args.get('fee_asset')
    kwargs['notes'] = named_args.get('notes')
    kwargs['transaction_id_blockchain'] = named_args.get('tx_id')

    logger.info(f"[HANDLER] Данные подготовлены. Вызываю log_fund_movement...")
    success, message = log_fund_movement(
        movement_type=move_type, asset=asset, amount=amount_dec, timestamp=timestamp_obj, **kwargs
    )

    if success:
        await update.message.reply_text(f"✅ Операция {move_type.lower()} на {amount_dec} {asset} залогирована.", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(f"❌ {message}", parse_mode=ParseMode.HTML)


async def deposit_command(update: Update, context: CallbackContext) -> None:
    await movement_command(update, context, move_type='DEPOSIT')


async def withdraw_command(update: Update, context: CallbackContext) -> None:
    await movement_command(update, context, move_type='WITHDRAWAL')


async def transfer_command(update: Update, context: CallbackContext) -> None:
    await movement_command(update, context, move_type='TRANSFER')


# --- [ОБНОВЛЕНО] Команды отчетов ---
@admin_only
async def portfolio_command(update: Update, context: CallbackContext) -> None:
    logger.info("[ОТЛАДКА] Вызвана команда /portfolio")
    
    positions, errors = sheets_service.get_all_records(config.OPEN_POSITIONS_SHEET_NAME, PositionData)
    
    if errors:
        await update.message.reply_text(f"❌ Ошибка чтения позиций: {errors[0]}")
        return

    if not positions:
        await update.message.reply_text("Нет открытых позиций.")
        return
        
    reply_text = "<u><b>💼 Открытые Позиции:</b></u>\n\n"
    for pos in positions:
        pnl_val = utils.parse_decimal(pos.unrealized_pnl) or Decimal('0')
        pnl_str = f"{pnl_val:+.2f}"
        
        reply_text += (f"<b>{pos.symbol}</b> ({pos.exchange})\n"
                       f"  Кол-во: <code>{pos.net_amount:.4f}</code>\n"
                       f"  Ср.вход: <code>{pos.avg_entry_price:.4f}</code>\n"
                       f"  Нереал.PNL: <code>{pnl_str} $</code>\n\n")
                       
    await update.message.reply_text(reply_text, parse_mode=ParseMode.HTML)


@admin_only
async def history_command(update: Update, context: CallbackContext) -> None:
    """
    [ИСПРАВЛЕНО] Показывает историю сделок по тикеру,
    используя "умный" поиск (например, 'AVAX' найдет и 'AVAX', и 'AVAX/USDT').
    """
    if not context.args:
        await update.message.reply_text("Использование: <code>/history SYMBOL</code>", parse_mode=ParseMode.HTML)
        return

    symbol_to_find = context.args[0].upper()
    
    all_trades, errors = sheets_service.get_all_records(config.CORE_TRADES_SHEET_NAME, TradeData)

    if errors:
        await update.message.reply_text(f"❌ Ошибка чтения истории: {errors[0]}")
        return

    trades = [
        t for t in all_trades if t.symbol and (
            t.symbol.upper() == symbol_to_find or
            t.symbol.upper().startswith(symbol_to_find + '/')
        )
    ]
    
    if not trades:
        await update.message.reply_text(f"Нет истории сделок для {symbol_to_find}.")
        return

    trades.sort(key=lambda t: t.timestamp, reverse=True)
    
    reply_text = f"<u><b>📜 История сделок для {symbol_to_find} (макс. 10):</b></u>\n"
    for trade in trades[:10]:
        reply_text += (f"<pre>{trade.timestamp:%Y-%m-%d %H:%M} {trade.trade_type:<4} "
                       f"{trade.amount:<8.4f} {trade.symbol:<10} @ {trade.price:<10.4f}</pre>\n")
                       
    await update.message.reply_text(reply_text, parse_mode=ParseMode.HTML)


@admin_only
async def average_command(update: Update, context: CallbackContext) -> None:
    """[ИСПРАВЛЕНО] Ищет позицию по точному совпадению или по базовому активу."""
    if not context.args:
        await update.message.reply_text("Использование: <code>/average SYMBOL</code>", parse_mode=ParseMode.HTML)
        return

    symbol_to_find = context.args[0].upper()
    
    all_positions, errors = sheets_service.get_all_records(config.OPEN_POSITIONS_SHEET_NAME, PositionData)
    
    if errors:
        await update.message.reply_text(f"❌ Ошибка чтения позиций: {errors[0]}")
        return

    position = next(
        (p for p in all_positions if p.symbol and (
            p.symbol.upper() == symbol_to_find or 
            p.symbol.upper().startswith(symbol_to_find + '/'))
        ), None)

    if not position:
        await update.message.reply_text(f"Нет открытой позиции для {symbol_to_find}.")
        return

    reply_text = (f"<u><b>📊 Средняя цена для {position.symbol}:</b></u>\n"
                  f"  Общее кол-во: <code>{position.net_amount:.4f}</code>\n"
                  f"  Средняя цена входа: <code>{position.avg_entry_price:.4f}</code>\n")
    await update.message.reply_text(reply_text, parse_mode=ParseMode.HTML)


@admin_only
async def updater_status_command(update: Update, context: CallbackContext) -> None:
    """
    [ИСПРАВЛЕНО] Показывает статус фонового сервиса обновления цен,
    корректно обрабатывая возможные ошибки.
    """
    try:
        status, timestamp = sheets_service.get_system_status()

        if status is None and timestamp is None:
            await update.message.reply_text("🟡 Price Updater: нет данных о статусе. Возможно, сервис еще ни разу не запускался.")
            return

        status_str = status or "N/A"
        timestamp_str = timestamp or "N/A"

        reply_msg = f"🟢 Price Updater:\n- Статус: <b>{status_str}</b>\n- Посл. обновление: <b>{timestamp_str}</b>"
        await update.message.reply_text(reply_msg, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(f"Ошибка в команде /updater_status: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Произошла ошибка при получении статуса: {e}")


@admin_only
async def update_analytics_command(update: Update, context: CallbackContext) -> None:
    await update.message.reply_text("⚙️ Запускаю полное обновление аналитики...", parse_mode=ParseMode.HTML)
    success, message = analytics_service.calculate_and_update_analytics_sheet()
    if success:
        await update.message.reply_text(f"✅ Обновление аналитики завершено!\n{message}", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(f"❌ Ошибка обновления аналитики:\n{message}", parse_mode=ParseMode.HTML)


# [НОВАЯ ФУНКЦИЯ]
@admin_only
async def balance_command(update: Update, context: CallbackContext) -> None:
    """Отправляет сводку по балансам стейблкоинов."""
    logger.info("[HANDLER] Получена команда /balance")
    
    all_balances, errors = sheets_service.get_all_records(config.ACCOUNT_BALANCES_SHEET_NAME, BalanceData)
    
    if errors:
        await update.message.reply_text(f"❌ Ошибка чтения балансов: {errors[0]}")
        return

    stable_balances = [b for b in all_balances if b.asset in config.INVESTMENT_ASSETS and b.balance > 0]

    if not stable_balances:
        await update.message.reply_text("Нет данных о балансах стейблкоинов.")
        return

    # Группируем балансы по счетам
    balances_by_account = {}
    for balance in stable_balances:
        account_name = balance.account_name or "Без имени"
        if account_name not in balances_by_account:
            balances_by_account[account_name] = []
        balances_by_account[account_name].append(balance)

    total_stables_value = sum(b.balance for b in stable_balances if b.balance)
    
    reply_text = f"<u><b>💰 Сводка по стейблкоинам:</b></u>\n"
    reply_text += f"<b>Итого:</b> <code>{total_stables_value:,.2f} $</code>\n\n"

    for account, balances in balances_by_account.items():
        account_total = sum(b.balance for b in balances if b.balance)
        reply_text += f"<b>📍 {account.capitalize()}:</b> <code>{account_total:,.2f} $</code>\n"
        for b in balances:
            reply_text += f"  - {b.asset}: <code>{b.balance:,.2f}</code>\n"
        reply_text += "\n"

    await update.message.reply_text(reply_text, parse_mode=ParseMode.HTML)