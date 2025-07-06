# deal_tracker/price_updater_ccxt.py
import logging
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import List, Dict, Tuple
from collections import defaultdict
import ccxt
import sheets_service
import config
from models import PositionData

# --- Настройка логирования ---
logging.basicConfig(level=config.LOG_LEVEL, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def fetch_all_prices(positions: List[PositionData]) -> Dict[str, Dict[str, Decimal]]:
    """Загружает актуальные цены для всех позиций, группируя по биржам."""
    if not positions:
        return {}
        
    symbols_by_exchange = defaultdict(list)
    for pos in positions:
        if pos.exchange and pos.symbol:
            symbols_by_exchange[pos.exchange.lower()].append(pos.symbol)

    all_prices = defaultdict(dict)
    for exchange_id, symbols in symbols_by_exchange.items():
        try:
            exchange_class = getattr(ccxt, exchange_id, None)
            if not exchange_class:
                logger.warning(f"Биржа {exchange_id} не найдена в CCXT.")
                continue
            
            exchange = exchange_class()
            tickers = exchange.fetch_tickers(list(set(symbols)))
            
            for symbol, ticker in tickers.items():
                if ticker and ticker.get('last') is not None:
                    all_prices[exchange_id][symbol] = Decimal(str(ticker['last']))
        except Exception as e:
            logger.error(f"Не удалось получить цены с биржи {exchange_id}: {e}")
            continue
            
    return dict(all_prices)


def update_prices_and_pnl() -> Tuple[bool, str]:
    """
    Основная функция: загружает позиции, обновляет цены и PNL.
    Использует пакетную загрузку и пакетное обновление.
    """
    logger.info("Запуск обновления цен и PNL...")
    
    # Используем get_all_records, который является оберткой над batch_get_records
    open_positions, errors = sheets_service.get_all_records(config.OPEN_POSITIONS_SHEET_NAME, PositionData)
    if errors:
        return False, f"Ошибка чтения позиций: {errors}"
    if not open_positions:
        return True, "Нет открытых позиций для обновления."

    current_prices = fetch_all_prices(open_positions)
    positions_to_update = []
    
    for pos in open_positions:
        price = current_prices.get(pos.exchange.lower(), {}).get(pos.symbol)
        if price:
            pos.current_price = price
            pos.unrealized_pnl = (price - pos.avg_entry_price) * pos.net_amount
            pos.last_updated = datetime.now()
            positions_to_update.append(pos)

    if not positions_to_update:
        return True, "Не удалось обновить цены ни для одной из позиций."

    # Вызываем новую функцию для пакетного обновления
    if not sheets_service.batch_update_positions(positions_to_update):
        return False, "Ошибка при пакетной записи обновленных позиций."

    msg = f"Успешно обновлены цены для {len(positions_to_update)} позиций."
    logger.info(msg)
    return True, msg


# --- Главный блок запуска ---
if __name__ == "__main__":
    logger.info("Сервис обновления цен запущен в циклическом режиме.")
    while True:
        try:
            success, message = update_prices_and_pnl()
            status = "OK" if success else "ERROR"
            
            target_timezone = timezone(timedelta(hours=config.TZ_OFFSET_HOURS))
            timestamp = datetime.now(timezone.utc).astimezone(target_timezone)
            sheets_service.update_system_status(status, timestamp)
            
            if success:
                logger.info(f"Цикл обновления цен завершен. {message}")
            else:
                logger.error(f"Цикл обновления цен завершился с ошибкой: {message}")
        except Exception as e:
            logger.critical(f"Критическая ошибка в главном цикле обновления цен: {e}", exc_info=True)
            sheets_service.update_system_status("CRITICAL_ERROR", datetime.now())
        
        logger.info(f"Следующий запуск через {config.PRICE_UPDATE_INTERVAL_SECONDS} секунд.")
        time.sleep(config.PRICE_UPDATE_INTERVAL_SECONDS)
