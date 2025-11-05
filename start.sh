#!/bin/bash
set -e

# Обновляем pip
pip install --upgrade pip

# Устанавливаем зависимости (только готовые бинарники)
pip install --only-binary :all: -r requirements.txt

# Запуск приложения
exec uvicorn main:app --host ${HOST:-0.0.0.0} --port ${PORT:-8000}
