# deal_tracker/price_updater_ccxt.py
import asyncio
import logging
import os
import time
import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import gspread

import ccxt.async_support as ccxt_async

try:
    import sheets_service
    import notifier
    import config
except ImportError as e:
    logging.basicConfig(level=logging.ERROR)
    logging.error(f"Критическая ошибка импорта: {e}")
    exit(1)

# --- Константы точности ---
PRICE_PRECISION_STR_PU = getattr(config, 'PRICE_DISPLAY_PRECISION', "0.000001")
USD_PRECISION_STR_PU = getattr(config, 'USD_DISPLAY_PRECISION', '0.01')

# --- Настройка логгера ---
LOGS_DIR_PATH = getattr(config, 'LOGS_DIR', 'logs')
os.makedirs(LOGS_DIR_PATH, exist_ok=True)
LOG_FILE_NAME = getattr(config, 'PRICE_UPDATER_LOG_FILE', 'price_updater.log')
log_file_path = os.path.join(LOGS_DIR_PATH, LOG_FILE_NAME)

logger_main = logging.getLogger(__name__)
if not logger_main.handlers:
    logger_main.setLevel(getattr(config, 'LOG_LEVEL', logging.INFO))
    file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    logger_main.addHandler(file_handler)
    logger_main.addHandler(console_handler)

# --- Вспомогательные функции ---


def _safe_decimal_updater(value, default_if_error=Decimal('0')) -> Decimal:
    """Безопасно конвертирует значение в Decimal, обрабатывая запятые и пробелы."""
    if value is None or str(value).strip() == '':
        return default_if_error
    try:
        # Сначала убираем пробелы, затем меняем запятую на точку
        clean_value = str(value).replace(' ', '').replace(',', '.')
        return Decimal(clean_value)
    except InvalidOperation:
        logger_main.debug(
            f"Ошибка конвертации '{value}' в Decimal, используется default: {default_if_error}")
        return default_if_error


ccxt_exchange_cache = {}


async def get_ccxt_exchange(exchange_name: str):
    """Возвращает инициализированный экземпляр CCXT, используя кэш."""
    if exchange_name in ccxt_exchange_cache:
        return ccxt_exchange_cache[exchange_name]
    try:
        exchange_class = getattr(ccxt_async, exchange_name.lower())
        exchange = exchange_class()
        ccxt_exchange_cache[exchange_name] = exchange
        logger_main.info(
            f"Инициализирован и закэширован экземпляр CCXT для {exchange_name}")
        return exchange
    except AttributeError:
        logger_main.error(f"Биржа {exchange_name} не найдена в CCXT.")
        return None
    except Exception as e:
        logger_main.error(f"Ошибка инициализации биржи {exchange_name}: {e}")
        return None


async def close_all_ccxt_exchanges():
    """Закрывает все закэшированные сессии CCXT."""
    for exchange_name, exchange_instance in ccxt_exchange_cache.items():
        try:
            if hasattr(exchange_instance, 'close') and asyncio.iscoroutinefunction(exchange_instance.close):
                await exchange_instance.close()
                logger_main.info(f"CCXT сессия для {exchange_name} закрыта.")
        except Exception as e:
            logger_main.error(
                f"Ошибка при закрытии CCXT сессии для {exchange_name}: {e}")
    ccxt_exchange_cache.clear()


async def fetch_current_price(exchange_ccxt_instance, symbol: str) -> Decimal | None:
    """Получает текущую цену для символа с указанной биржи."""
    if not exchange_ccxt_instance:
        return None
    try:
        ticker = await exchange_ccxt_instance.fetch_ticker(symbol)
        if ticker and 'last' in ticker and ticker['last'] is not None:
            return Decimal(str(ticker['last']))
        else:
            logger_main.warning(
                f"Не удалось получить 'last' цену для {symbol} на {exchange_ccxt_instance.id}.")
            return None
    except ccxt_async.NetworkError as e:
        logger_main.error(
            f"Сетевая ошибка CCXT для {symbol} на {exchange_ccxt_instance.id}: {e}")
    except ccxt_async.ExchangeError as e:
        logger_main.error(
            f"Ошибка биржи CCXT для {symbol} на {exchange_ccxt_instance.id}: {e}")
    except Exception as e:
        logger_main.error(
            f"Общая ошибка CCXT для {symbol} на {exchange_ccxt_instance.id}: {e}")
    return None


async def update_prices_and_pnl():
    """Главная функция, которая получает позиции, запрашивает цены и обновляет PNL в таблице."""
    logger_main.info("Запуск цикла обновления цен и PNL...")
    update_successful_overall = True
    try:
        open_positions = sheets_service.get_all_open_positions()
        if not open_positions:
            logger_main.info("Нет открытых позиций для обновления.")
            return

        # ИЗМЕНЕНО: Получаем объект листа один раз в начале
        sheet = sheets_service.get_sheet_by_name(
            config.OPEN_POSITIONS_SHEET_NAME)
        if not sheet:
            logger_main.error(
                f"Не удалось получить лист {config.OPEN_POSITIONS_SHEET_NAME}. Обновление прервано.")
            return

        headers = sheet.row_values(1)
        if not headers:
            logger_main.error(
                f"Не удалось прочитать заголовки из {config.OPEN_POSITIONS_SHEET_NAME}.")
            return

        # ИЗМЕНЕНО: Простой и надежный поиск индекса колонок
        try:
            current_price_col = headers.index('Current_Price') + 1
            unrealized_pnl_col = headers.index('Unrealized_PNL') + 1
        except ValueError as e:
            logger_main.error(
                f"Критическая ошибка: не найдена одна из обязательных колонок ('Current_Price' или 'Unrealized_PNL') в {config.OPEN_POSITIONS_SHEET_NAME}: {e}")
            return

        batch_update_payload = []
        processed_symbols_for_log = []

        for position in open_positions:
            symbol = position.get('Symbol')
            exchange = position.get('Exchange', '').strip()
            row_number = position.get('row_number')

            if not all([symbol, exchange, row_number]):
                logger_main.warning(
                    f"Пропуск позиции с неполными данными: {position}")
                continue

            exchange_instance = await get_ccxt_exchange(exchange)
            current_price = await fetch_current_price(exchange_instance, symbol)

            if current_price is None:
                continue

            net_amount = _safe_decimal_updater(position.get('Net_Amount'))
            avg_entry_price = _safe_decimal_updater(
                position.get('Avg_Entry_Price'))

            if avg_entry_price.is_zero():
                unrealized_pnl = Decimal('0')
            else:
                unrealized_pnl = (current_price - avg_entry_price) * net_amount

            price_to_write = str(current_price.quantize(
                Decimal(PRICE_PRECISION_STR_PU), rounding=ROUND_HALF_UP))
            pnl_to_write = str(unrealized_pnl.quantize(
                Decimal(USD_PRECISION_STR_PU), rounding=ROUND_HALF_UP))

            batch_update_payload.append({'range': gspread.utils.rowcol_to_a1(
                row_number, current_price_col), 'values': [[price_to_write]]})
            batch_update_payload.append({'range': gspread.utils.rowcol_to_a1(
                row_number, unrealized_pnl_col), 'values': [[pnl_to_write]]})
            processed_symbols_for_log.append(f"{symbol}({exchange})")

        if batch_update_payload:
            try:
                sheet.batch_update(batch_update_payload,
                                   value_input_option='USER_ENTERED')
                logger_main.info(
                    f"Пакетно обновлены цены/PNL для: {', '.join(processed_symbols_for_log)}")
            except Exception as e_batch:
                logger_main.error(
                    f"Ошибка пакетного обновления цен/PNL: {e_batch}")
                update_successful_overall = False

    except Exception as e:
        logger_main.error(
            f"Критическая ошибка в цикле обновления цен: {e}", exc_info=True)
        update_successful_overall = False
        if notifier:
            await notifier.send_telegram_alert(f"🆘 Критическая ошибка в price_updater_ccxt: {e}")

    finally:
        current_time_str = (datetime.datetime.now(datetime.timezone.utc) +
                            datetime.timedelta(hours=config.TZ_OFFSET_HOURS)).strftime("%Y-%m-%d %H:%M:%S")
        status_to_write = "OK" if update_successful_overall else "ERROR"

        # ИЗМЕНЕНО: Используем новую функцию update_cell
        sheets_service.update_cell(
            config.SYSTEM_STATUS_SHEET_NAME, config.UPDATER_LAST_RUN_CELL, current_time_str)
        sheets_service.update_cell(
            config.SYSTEM_STATUS_SHEET_NAME, config.UPDATER_STATUS_CELL, status_to_write)

        logger_main.info(
            f"Цикл обновления цен завершен. Статус: {status_to_write}. Время записано: {current_time_str}")


async def main_loop():
    """Главный цикл, запускающий обновление цен с заданным интервалом."""
    update_interval = getattr(config, 'PRICE_UPDATE_INTERVAL_SECONDS', 300)
    logger_main.info(
        f"Price updater запущен. Интервал обновления: {update_interval} секунд.")
    if notifier and hasattr(notifier, 'send_telegram_alert'):
        await notifier.send_telegram_alert(f"📈 Price Updater запущен (интервал {update_interval}с).")
    else:
        logger_main.warning("Notifier или send_telegram_alert не настроен.")

    while True:
        await update_prices_and_pnl()
        logger_main.info(
            f"Ожидание следующего обновления через {update_interval} секунд...")
        await asyncio.sleep(update_interval)

if __name__ == '__main__':
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger_main.info("Price updater остановлен вручную.")
    except Exception as e_main:
        logger_main.critical(
            f"Критическая ошибка остановила главный цикл price_updater: {e_main}", exc_info=True)
        if notifier and hasattr(notifier, 'send_telegram_alert'):
            try:
                # Попытка асинхронно отправить уведомление об остановке
                async def notify_critical_stop():
                    await notifier.send_telegram_alert(f"🆘 Price Updater ОСТАНОВЛЕН из-за критической ошибки: {e_main}")
                asyncio.run(notify_critical_stop())
            except Exception as e_notify_final:
                logger_main.error(
                    f"Не удалось отправить финальное уведомление об ошибке: {e_notify_final}")
    finally:
        logger_main.info(
            "Завершение работы price_updater, закрытие CCXT сессий...")
        asyncio.run(close_all_ccxt_exchanges())
        logger_main.info("CCXT сессии закрыты. Price updater завершен.")
