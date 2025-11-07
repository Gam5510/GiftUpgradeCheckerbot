#!/bin/bash
# Запуск приложения
exec uvicorn main:app --host ${HOST:-0.0.0.0} --port ${PORT:-8000}
