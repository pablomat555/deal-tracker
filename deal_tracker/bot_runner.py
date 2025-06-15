# deal_tracker/bot_runner.py
import logging
import os
# import asyncio # Не используется напрямую здесь

from telegram.ext import Application, CommandHandler
# Убраны MessageHandler, filters, если не используются для обработки неизвестных команд
# ParseMode теперь используется только в telegram_handlers.py

import config  # config импортируется первым для настройки логирования из него, если есть
from telegram_handlers import (
    start_command, help_command,
    buy_command, sell_command,
    deposit_command, withdraw_command,  # Новые deposit/withdraw
    transfer_command,
    portfolio_command, history_command, average_command,
    # cashflow_command, # <--- УДАЛЕНО
    movements_command,
    updater_status_command, update_analytics_command
)

# --- Диагностика версии (можно закомментировать после успешного запуска) ---
# try:
#     import telegram
#     print(f"--- Версия python-telegram-bot: {telegram.__version__} ---")
# except ImportError:
#     print("--- Не удалось импортировать библиотеку telegram ---")
# --- Конец диагностики ---

# Настройка логирования (основная настройка здесь)
# Предполагается, что config.LOGS_DIR и config.BOT_LOG_FILE определены в config.py
# config.LOG_LEVEL также должен быть определен в config.py
os.makedirs(getattr(config, 'LOGS_DIR', 'logs'), exist_ok=True)
log_file_path = os.path.join(
    getattr(config, 'LOGS_DIR', 'logs'),
    getattr(config, 'BOT_LOG_FILE', 'bot.log')
)

# Вызываем basicConfig только один раз
# Убедитесь, что в config.py нет конкурирующего вызова basicConfig,
# или он защищен if not logging.getLogger().hasHandlers()
if not logging.getLogger().hasHandlers():  # Проверяем, не настроены ли уже обработчики
    logging.basicConfig(
        # Используем уровень из конфига
        level=getattr(config, 'LOG_LEVEL', logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file_path, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

# Уменьшение "болтливости" библиотечных логгеров
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(
    logging.INFO)  # Оставляем INFO для telegram.ext
logging.getLogger("apscheduler.scheduler").setLevel(
    logging.INFO)  # Для apscheduler тоже можно INFO
logging.getLogger('apscheduler.executors.default').setLevel(logging.WARNING)


logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("Запуск Telegram бота...")

    if not config.TELEGRAM_TOKEN or config.TELEGRAM_TOKEN == 'ВАШ_ТЕЛЕГРАМ_ТОКЕН':
        logger.critical(
            "TELEGRAM_TOKEN не найден или не изменен в конфигурации. Бот не может быть запущен.")
        return

    if not config.TELEGRAM_CHAT_ID or config.TELEGRAM_CHAT_ID == "0" or config.TELEGRAM_CHAT_ID == "ВАШ_ТЕЛЕГРАМ_CHAT_ID":
        logger.warning(
            "TELEGRAM_CHAT_ID не настроен или не изменен. Команды с @admin_only могут работать некорректно.")

    application = (
        Application.builder()
        .token(config.TELEGRAM_TOKEN)
        .build()
    )

    # Регистрация обработчиков команд
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))

    application.add_handler(CommandHandler("buy", buy_command))
    application.add_handler(CommandHandler("sell", sell_command))

    application.add_handler(CommandHandler("deposit", deposit_command))
    application.add_handler(CommandHandler("withdraw", withdraw_command))
    application.add_handler(CommandHandler("transfer", transfer_command))

    application.add_handler(CommandHandler("portfolio", portfolio_command))
    application.add_handler(CommandHandler("history", history_command))
    application.add_handler(CommandHandler("average", average_command))
    # application.add_handler(CommandHandler("cashflow", cashflow_command)) # <--- УДАЛЕНО
    application.add_handler(CommandHandler("movements", movements_command))

    application.add_handler(CommandHandler(
        "updater_status", updater_status_command))
    application.add_handler(CommandHandler(
        "update_analytics", update_analytics_command))

    logger.info("Бот запущен и готов принимать команды.")
    application.run_polling()
    logger.info("Бот остановлен.")


if __name__ == '__main__':
    main()
