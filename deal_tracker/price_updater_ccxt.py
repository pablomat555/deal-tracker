# deal_tracker/price_updater_ccxt.py
import asyncio
import logging
import os
import datetime
from decimal import Decimal
from typing import List

import ccxt.async_support as ccxt_async

# Импортируем наши новые, чистые модули
import sheets_service
import config
from models import PositionData

# --- Настройка логгера ---
# (Код настройки логгера остается без изменений, можно скопировать из вашей версии)
logger = logging.getLogger(__name__)


# --- Логика работы с CCXT ---
ccxt_exchange_cache = {}


async def get_ccxt_exchange(exchange_name: str):
    """Возвращает инициализированный экземпляр CCXT, используя кэш."""
    if exchange_name in ccxt_exchange_cache:
        return ccxt_exchange_cache[exchange_name]
    try:
        exchange_class = getattr(ccxt_async, exchange_name.lower())
        exchange = exchange_class()
        ccxt_exchange_cache[exchange_name] = exchange
        logger.info(f"Инициализирован экземпляр CCXT для {exchange_name}")
        return exchange
    except AttributeError:
        logger.error(f"Биржа {exchange_name} не найдена в CCXT.")
        return None


async def close_all_ccxt_exchanges():
    """Закрывает все закэшированные сессии CCXT."""
    for name, instance in ccxt_exchange_cache.items():
        if hasattr(instance, 'close'):
            await instance.close()
            logger.info(f"CCXT сессия для {name} закрыта.")
    ccxt_exchange_cache.clear()


async def fetch_current_price(exchange_instance, symbol: str) -> Decimal | None:
    """Получает текущую цену для символа с указанной биржи."""
    if not exchange_instance:
        return None
    try:
        ticker = await exchange_instance.fetch_ticker(symbol)
        if ticker and 'last' in ticker and ticker['last'] is not None:
            return Decimal(str(ticker['last']))
        logger.warning(
            f"Не удалось получить цену для {symbol} на {exchange_instance.id}.")
    except Exception as e:
        logger.error(
            f"Ошибка CCXT для {symbol} на {exchange_instance.id}: {e}")
    return None


async def update_prices_and_pnl():
    """Главная функция: получает позиции, запрашивает цены и обновляет PNL."""
    logger.info("Запуск цикла обновления цен...")
    update_successful = True

    try:
        # 1. Получаем список объектов PositionData
        open_positions: List[PositionData] = sheets_service.get_all_open_positions(
        )
        if not open_positions:
            logger.info("Нет открытых позиций для обновления.")
            # Важно вернуть True, т.к. ошибки не было, просто нет работы
            update_successful = True
            return

        updated_positions: List[PositionData] = []

        for position in open_positions:
            # 2. Работаем с чистыми данными из моделей
            if not all([position.symbol, position.exchange, position.net_amount, position.avg_entry_price]):
                logger.warning(
                    f"Пропуск позиции с неполными данными: {position}")
                continue

            exchange_instance = await get_ccxt_exchange(position.exchange)
            current_price = await fetch_current_price(exchange_instance, position.symbol)

            if current_price is None:
                continue

            # 3. Рассчитываем PNL напрямую, без парсинга строк
            unrealized_pnl = (
                current_price - position.avg_entry_price) * position.net_amount

            # 4. Обновляем сам объект модели
            position.current_price = current_price
            position.unrealized_pnl = unrealized_pnl
            position.last_updated = datetime.datetime.now()

            updated_positions.append(position)

        # 5. Отправляем все обновленные объекты на пакетную запись
        if updated_positions:
            if not sheets_service.batch_update_positions(updated_positions):
                update_successful = False
                logger.error("Ошибка во время пакетного обновления позиций.")

    except Exception as e:
        logger.error(
            f"Критическая ошибка в цикле обновления цен: {e}", exc_info=True)
        update_successful = False
    finally:
        # 6. Обновляем статус с помощью новой, безопасной функции
        timestamp = datetime.datetime.now(datetime.timezone.utc).astimezone(
            datetime.timezone(datetime.timedelta(hours=config.TZ_OFFSET_HOURS))
        )
        status = "OK" if update_successful else "ERROR"

        # --- ВОТ ЭТА СТРОКА БЫЛА ПРОПУЩЕНА ---
        sheets_service.update_system_status(status, timestamp)
        # ------------------------------------


async def main_loop():
    """Главный цикл, запускающий обновление цен с заданным интервалом."""
    update_interval = config.PRICE_UPDATE_INTERVAL_SECONDS
    logger.info(f"Price updater запущен. Интервал: {update_interval} секунд.")

    while True:
        await update_prices_and_pnl()
        logger.info(
            f"Ожидание следующего обновления через {update_interval} секунд...")
        await asyncio.sleep(update_interval)

if __name__ == '__main__':
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Price updater остановлен вручную.")
    finally:
        logger.info("Завершение работы price_updater, закрытие сессий...")
        asyncio.run(close_all_ccxt_exchanges())
        logger.info("Price updater завершен.")
