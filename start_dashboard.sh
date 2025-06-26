#!/bin/bash
# Переходим в директорию проекта
cd /root/apps/deal_tracker_v1.0

# Активируем виртуальное окружение
source .venv/bin/activate

# Запускаем Streamlit как модуль на порту 8501
python3 -m streamlit run deal_tracker/dashboard.py --server.port 8501
