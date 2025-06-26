# config.py

import logging
import os
from dotenv import load_dotenv
from pathlib import Path

# [ИСПРАВЛЕНО] Явное указание пути к .env файлу
# Это делает поиск .env файла более надежным, независимо от того, откуда запускается скрипт.
# Path(__file__).parent -> папка /deal_tracker
# .parent -> корневая папка проекта
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)


# --- Основные настройки ---
# Смещение временной зоны в часах (например, 3 для UTC+3)
TZ_OFFSET_HOURS = int(os.getenv('TZ_OFFSET_HOURS', '3'))

# --- Валюта учета ---
# Основная валюта, в которой будут агрегироваться ключевые финансовые показатели
BASE_CURRENCY = os.getenv('BASE_CURRENCY', 'USD').upper()

# --- Настройки Telegram ---
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', 'ВАШ_ТЕЛЕГРАМ_ТОКЕН')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', 'ВАШ_ТЕЛЕГРАМ_CHAT_ID')
# Для нескольких администраторов через запятую. Если пусто, используется TELEGRAM_CHAT_ID.
TELEGRAM_ADMIN_IDS_STR = os.getenv('TELEGRAM_ADMIN_IDS_STR', TELEGRAM_CHAT_ID)

# --- Настройки Google Sheets ---
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID', 'ВАШ_SPREADSHEET_ID')

# Название переменной приведено в соответствие с sheets_service.py
GOOGLE_CREDS_JSON_PATH = os.getenv(
    'GOOGLE_CREDS_JSON_PATH', 'credentials.json')

# --- Имена листов в Google Sheets ---
# Эти переменные теперь будут использоваться в sheets_service.py
CORE_TRADES_SHEET_NAME = os.getenv('CORE_TRADES_SHEET_NAME', 'Core_Trades')
OPEN_POSITIONS_SHEET_NAME = os.getenv(
    'OPEN_POSITIONS_SHEET_NAME', 'Open_Positions')
FUND_MOVEMENTS_SHEET_NAME = os.getenv(
    'FUND_MOVEMENTS_SHEET_NAME', 'Fund_Movements')
FIFO_LOG_SHEET_NAME = os.getenv('FIFO_LOG_SHEET_NAME', 'Fifo_Log')
ANALYTICS_SHEET_NAME = os.getenv('ANALYTICS_SHEET_NAME', 'Analytics')
SYSTEM_STATUS_SHEET_NAME = os.getenv(
    'SYSTEM_STATUS_SHEET_NAME', 'System_Status')
ACCOUNT_BALANCES_SHEET_NAME = os.getenv(
    'ACCOUNT_BALANCES_SHEET_NAME', 'Account_Balances')


# --- Настройки точности для Decimal ---
USD_PRECISION_STR_LOGGING = os.getenv(
    'USD_PRECISION_STR_LOGGING', '0.00000001')
QTY_PRECISION_STR_LOGGING = os.getenv(
    'QTY_PRECISION_STR_LOGGING', '0.0000000001')
PRICE_PRECISION_STR_LOGGING = os.getenv(
    'PRICE_PRECISION_STR_LOGGING', '0.0000000001')

USD_DISPLAY_PRECISION = os.getenv('USD_DISPLAY_PRECISION', '0.01')
QTY_DISPLAY_PRECISION = os.getenv('QTY_DISPLAY_PRECISION', '0.0001')
PRICE_DISPLAY_PRECISION = os.getenv('PRICE_DISPLAY_PRECISION', '0.0001')

# --- Имена по умолчанию для команд /deposit и /withdraw ---
DEFAULT_DEPOSIT_SOURCE_NAME = os.getenv(
    'DEFAULT_DEPOSIT_SOURCE_NAME', "External Inflow")
DEFAULT_WITHDRAW_DESTINATION_NAME = os.getenv(
    'DEFAULT_WITHDRAW_DESTINATION_NAME', "External Outflow")
DEFAULT_INTERNAL_ACCOUNT_NAME = os.getenv(
    'DEFAULT_INTERNAL_ACCOUNT_NAME', "Основной Счет")

DEFAULT_EXTERNAL_ENTITY_TYPE = "EXTERNAL"
DEFAULT_INTERNAL_ACCOUNT_TYPE = "INTERNAL_ACCOUNT"

DEFAULT_DEPOSIT_WITHDRAW_ASSET = os.getenv(
    'DEFAULT_DEPOSIT_WITHDRAW_ASSET', BASE_CURRENCY)

# --- Списки известных бирж и кошельков (РАСШИРЕННЫЕ СПИСКИ) ---
KNOWN_EXCHANGES = [
    # Основные
    "Binance", "Bybit", "OKX", "KuCoin", "HTX", "Huobi", "Gate.io", "Kraken",
    "Coinbase", "Bitfinex", "MEXC", "Bitget", "WhiteBIT", "EXMO",
    # Для тестов
    "TestExch1", "TestExch2", "SomeExch"
]
KNOWN_WALLETS = [
    # Основные
    "MetaMask", "Trust Wallet", "Ledger", "Trezor", "Exodus", "Phantom",
    "Solflare", "MyEtherWallet", "Coinbase Wallet",
    # Кастомные
    "MyWallet", "Мой кошелек 1", "Аппаратный кошелек"
]

# --- Настройки для price_updater (если используется) ---
PRICE_UPDATE_INTERVAL_SECONDS = int(
    os.getenv('PRICE_UPDATE_INTERVAL_SECONDS', '300'))
UPDATER_LAST_RUN_CELL = os.getenv('UPDATER_LAST_RUN_CELL', 'A1')
UPDATER_STATUS_CELL = os.getenv('UPDATER_STATUS_CELL', 'B1')
PRICE_UPDATER_LOG_FILE = os.getenv(
    'PRICE_UPDATER_LOG_FILE', 'price_updater.log')

# --- Настройки аналитики ---
INVESTMENT_ASSETS = ['USD', 'USDT', 'USDC',
                     'DAI', 'BUSD', 'TUSD', 'USDP', 'FDUSD']

# --- Настройки логирования ---
LOG_LEVEL_STR = os.getenv('LOG_LEVEL', 'INFO').upper()
LOG_LEVEL = getattr(logging, LOG_LEVEL_STR, logging.INFO)
LOGS_DIR = os.getenv('LOGS_DIR', 'logs')
BOT_LOG_FILE = os.getenv('BOT_LOG_FILE', 'bot.log')