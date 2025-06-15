# deal_tracker/notifier.py
import logging
from telegram import Bot
from telegram.error import TelegramError
import asyncio  # Может понадобиться для запуска из синхронного кода, если такой будет

import config  # Для TELEGRAM_TOKEN и TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)

# Глобальный экземпляр бота для notifier, если он будет использоваться вне контекста Application
# Однако, лучше передавать экземпляр бота, если он доступен (например, из application.bot)
_bot_instance = None


def get_bot_instance():
    """
    Возвращает или создает и кэширует экземпляр Bot.
    """
    global _bot_instance
    if _bot_instance is None:
        if config.TELEGRAM_TOKEN:
            _bot_instance = Bot(token=config.TELEGRAM_TOKEN)
        else:
            logger.error(
                "Notifier: TELEGRAM_TOKEN не настроен. Невозможно создать экземпляр бота.")
    return _bot_instance


async def send_telegram_alert(message: str, bot_instance: Bot = None) -> bool:
    """
    Асинхронно отправляет сообщение администратору бота.

    Args:
        message (str): Текст сообщения для отправки.
        bot_instance (Bot, optional): Экземпляр telegram.Bot.
                                      Если None, будет использован глобальный или создан новый.

    Returns:
        bool: True, если сообщение успешно отправлено, иначе False.
    """
    current_bot = bot_instance if bot_instance else get_bot_instance()

    if not current_bot:
        logger.error(
            "Notifier: Экземпляр бота недоступен. Уведомление не отправлено.")
        return False

    if not config.TELEGRAM_CHAT_ID:  # Убедимся, что CHAT_ID есть
        logger.error(
            "Notifier: TELEGRAM_CHAT_ID не настроен. Уведомление не отправлено.")
        return False

    try:
        await current_bot.send_message(chat_id=config.TELEGRAM_CHAT_ID, text=message)
        logger.info(
            f"Уведомление успешно отправлено в чат {config.TELEGRAM_CHAT_ID}: \"{message[:50]}...\"")
        return True
    except TelegramError as e:
        logger.error(
            f"Notifier: Ошибка Telegram API при отправке уведомления в чат {config.TELEGRAM_CHAT_ID}: {e}")
        return False
    except Exception as e:
        logger.error(
            f"Notifier: Непредвиденная ошибка при отправке уведомления: {e}", exc_info=True)
        return False

# Если вам когда-нибудь понадобится отправлять уведомление из СИНХРОННОГО кода,
# можно использовать такую обертку. Но старайтесь избегать этого и использовать async вызовы.
# def send_telegram_alert_sync(message: str):
# try:
# loop = asyncio.get_event_loop()
# if loop.is_running():
# # Если цикл уже запущен, создаем задачу
#         # Этот подход может быть сложным и зависит от контекста
# # loop.create_task(send_telegram_alert(message))
# # Более простой, но блокирующий способ для редких случаев:
# asyncio.run(send_telegram_alert(message)) # Не рекомендуется часто
# else:
# asyncio.run(send_telegram_alert(message))
# except RuntimeError: # Если нет текущего event loop или другая проблема с asyncio
# logger.error("Notifier: Ошибка с event loop asyncio при синхронной отправке.")
# # Попытка создать новый цикл (может быть не всегда безопасно)
# try:
# asyncio.run(send_telegram_alert(message))
# except Exception as e_run:
# logger.error(f"Notifier: Ошибка при попытке asyncio.run в send_telegram_alert_sync: {e_run}")


if __name__ == '__main__':
    # Пример использования (для теста)
    # Убедитесь, что .env файл с TELEGRAM_TOKEN и TELEGRAM_CHAT_ID существует в корне проекта
    # и config.py правильно его читает.

    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    async def test_notifier():
        # Загрузка конфигурации, если еще не загружена (для прямого запуска notifier.py)
        from dotenv import load_dotenv
        import os
        # Переместите load_dotenv() и связанные импорты в начало файла, если они нужны глобально
        # или если вы часто запускаете этот файл напрямую для тестов.
        # Если config.py уже делает load_dotenv(), это может быть избыточным здесь.
        project_root = os.path.dirname(
            os.path.dirname(os.path.abspath(__file__)))
        dotenv_path = os.path.join(project_root, '.env')
        if os.path.exists(dotenv_path):
            load_dotenv(dotenv_path=dotenv_path)
            # Перезагружаем config, чтобы он подхватил переменные из .env, если notifier.py запускается сам по себе
            import importlib
            importlib.reload(config)
            logger.info(f".env файл загружен из: {dotenv_path}")
        else:
            logger.warning(f".env файл не найден по пути: {dotenv_path}")

        if not config.TELEGRAM_TOKEN or not config.TELEGRAM_CHAT_ID:
            logger.error(
                "Тестовый запуск notifier: TELEGRAM_TOKEN или TELEGRAM_CHAT_ID не установлены в config.")
            logger.info(
                f"Token: {'Есть' if config.TELEGRAM_TOKEN else 'Нет'}, Chat ID: {config.TELEGRAM_CHAT_ID}")
            return

        logger.info(
            f"Попытка отправить тестовое уведомление в чат ID: {config.TELEGRAM_CHAT_ID}")
        success = await send_telegram_alert("Тестовое уведомление от notifier.py! Бот функционирует.")
        if success:
            logger.info("Тестовое уведомление успешно отправлено.")
        else:
            logger.error("Не удалось отправить тестовое уведомление.")

    # Для запуска асинхронной функции test_notifier
    asyncio.run(test_notifier())
