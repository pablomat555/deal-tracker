# deal_tracker/bot_runner.py
import logging
import os
from telegram.ext import Application, CommandHandler

import config
from telegram_handlers import (
    start_command,
    help_command,
    buy_command,
    sell_command,
    deposit_command,
    withdraw_command,
    transfer_command,
    portfolio_command,
    history_command,
    average_command,
    updater_status_command,
    update_analytics_command
)

# Настройка логирования
os.makedirs(getattr(config, 'LOGS_DIR', 'logs'), exist_ok=True)
log_file_path = os.path.join(
    getattr(config, 'LOGS_DIR', 'logs'),
    getattr(config, 'BOT_LOG_FILE', 'bot.log')
)
if not logging.getLogger().hasHandlers():
    logging.basicConfig(
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
logging.getLogger("telegram.ext").setLevel(logging.INFO)
logging.getLogger('apscheduler.executors.default').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("Запуск Telegram бота...")

    if not config.TELEGRAM_TOKEN or config.TELEGRAM_TOKEN == 'ВАШ_ТЕЛЕГРАМ_ТОКЕН':
        logger.critical("TELEGRAM_TOKEN не найден. Бот не может быть запущен.")
        return

    application = Application.builder().token(config.TELEGRAM_TOKEN).build()

    # Регистрация обработчиков команд (только существующих)
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
    application.add_handler(CommandHandler(
        "updater_status", updater_status_command))
    application.add_handler(CommandHandler(
        "update_analytics", update_analytics_command))

    logger.info("Бот запущен и готов принимать команды.")
    application.run_polling()
    logger.info("Бот остановлен.")


if __name__ == '__main__':
    main()
