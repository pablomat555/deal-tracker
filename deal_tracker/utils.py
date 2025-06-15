# utils.py
from datetime import datetime
import pandas as pd
from decimal import Decimal, InvalidOperation
import config
import sheets_service
import streamlit as st

# --- ГЛАВНАЯ ФУНКЦИЯ ФОРМАТИРОВАНИЯ ЧИСЕЛ ---


def format_number(value, precision_str="0.01", add_plus_sign=False, show_currency_symbol=None):
    """
    Форматирует число в строку с пробелом как разделителем тысяч.
    Пример: 12345.67 -> "12 345.67"
    """
    try:
        val = Decimal(value)
        decimals = abs(Decimal(precision_str).as_tuple().exponent)
        formatted_str = f"{val:,.{decimals}f}".replace(',', ' ')

        if add_plus_sign and val > 0:
            formatted_str = f"+{formatted_str}"

        if show_currency_symbol:
            formatted_str = f"{formatted_str} {show_currency_symbol}"

        return formatted_str
    except (InvalidOperation, TypeError, ValueError):
        return "-"

# --- Остальные утилиты ---


def safe_to_decimal(value, default=Decimal('0')):
    """
    Безопасно конвертирует значение в Decimal,
    учитывая региональные форматы (пробелы как разделители тысяч, запятая как десятичный).
    """
    if value is None or value == '':
        return default
    try:
        s_value = str(value).replace(' ', '').replace(',', '.')
        return Decimal(s_value)
    except (InvalidOperation, TypeError, ValueError):
        return default


def style_pnl_value(val):
    """Стилизует ячейки PNL в зависимости от значения."""
    try:
        cleaned_val_str = ''.join(c for c in str(val) if c in '0123456789.-+')
        val_decimal = Decimal(cleaned_val_str)
        if val_decimal > 0:
            return 'color: #16A34A;'
        elif val_decimal < 0:
            return 'color: #DC2626;'
        else:
            return 'color: #6B7280;'
    except (InvalidOperation, ValueError, TypeError):
        return ''


def get_first_buy_trade_details(symbol, exchange, core_trades_data):
    """
    Заглушка: Здесь должна быть ваша логика поиска деталей первой сделки.
    """
    return {}

# --- ФУНКЦИЯ ЗАГРУЗКИ ДАННЫХ С КЭШИРОВАНИЕМ ---


@st.cache_data(ttl=300)
def load_all_dashboard_data():
    """
    Централизованно загружает все необходимые данные, вызывая корректные функции
    из sheets_service.py. Результат кэшируется.
    """
    # ИЗМЕНЕНО: Все вызовы приведены в соответствие с финальной версией sheets_service.py
    # (убраны суффиксы _records, изменен _fifo_records на _fifo_logs и т.д.)
    return {
        'analytics_history': sheets_service.get_analytics_history_records(),
        'open_positions': sheets_service.get_all_open_positions(),
        'core_trades': sheets_service.get_all_core_trades(),
        'fifo_logs': sheets_service.get_all_fifo_logs(),
        'fund_movements': sheets_service.get_all_fund_movements(),
        # Для балансов на дашборде нужны именно записи, а не map, поэтому вызываем универсальную функцию
        'account_balances': sheets_service.get_all_data_from_sheet(
            config.ACCOUNT_BALANCES_SHEET_NAME, sheets_service.ACCOUNT_BALANCES_FIELD_MAP
        )
    }
# utils.py


def parse_datetime_from_args(named_args: dict) -> datetime | None:  # type: ignore
    """
    Парсит дату из словаря именованных аргументов.
    Поддерживает форматы: ГГГГ-ММ-ДД, ГГГГ-ММ-ДД ЧЧ:ММ, ГГГГ-ММ-ДД ЧЧ:ММ:СС.
    Возвращает объект datetime или вызывает ValueError при ошибке формата.
    """
    date_input_str = named_args.get("date")
    if not date_input_str:
        return None

    date_input_str = date_input_str.strip()
    try:
        if len(date_input_str) == 19:
            return datetime.strptime(date_input_str, "%Y-%m-%d %H:%M:%S")
        elif len(date_input_str) == 16:
            return datetime.strptime(date_input_str, "%Y-%m-%d %H:%M")
        elif len(date_input_str) == 10:
            return datetime.strptime(date_input_str, "%Y-%m-%d")
        else:
            # Вызываем ошибку, если длина строки не соответствует ни одному формату
            raise ValueError("Неверная длина строки даты.")
    except ValueError as e:
        # Перехватываем и вызываем новую ошибку с более понятным сообщением
        raise ValueError(
            f"Неверный формат даты '{date_input_str}'. Ожидается ГГГГ-ММ-ДД[ ЧЧ:ММ[:СС]].") from e
