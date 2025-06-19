# deal_tracker/telegram_parser.py
import re
from typing import List, Dict, Tuple


def parse_command_args_advanced(args: List[str], num_positional_max: int) -> Tuple[List[str], Dict[str, str]]:
    """
    Продвинутый парсер аргументов команды.
    Разделяет аргументы на позиционные и именованные (ключ:значение).
    """
    positional_args = []
    named_args_dict = {}
    arg_idx = 0
    key_regex = r"^([a-zA-Z_а-яА-Я][a-zA-Z0-9_а-яА-Я]*):(.*)$"

    # Сбор позиционных аргументов
    while arg_idx < len(args):
        current_token = args[arg_idx]
        if re.match(key_regex, current_token) or len(positional_args) >= num_positional_max:
            break
        positional_args.append(current_token)
        arg_idx += 1

    # Сбор именованных аргументов
    current_key = None
    value_buffer = []
    while arg_idx < len(args):
        token = args[arg_idx]
        key_match = re.match(key_regex, token)
        if key_match:
            if current_key and value_buffer:
                named_args_dict[current_key] = " ".join(value_buffer).strip()
            current_key = key_match.group(1).lower()
            value_part = key_match.group(2).strip()
            value_buffer = []
            if value_part:
                if (value_part.startswith('"') and value_part.endswith('"')) or \
                   (value_part.startswith("'") and value_part.endswith("'")):
                    value_buffer.append(value_part[1:-1])
                else:
                    value_buffer.append(value_part)
        elif current_key:
            value_buffer.append(token)
        arg_idx += 1

    if current_key and value_buffer:
        named_args_dict[current_key] = " ".join(value_buffer).strip()
    elif current_key and not value_buffer:
        # Для ключей без значения, например 'flag:'
        named_args_dict[current_key] = ""

    return positional_args, named_args_dict
