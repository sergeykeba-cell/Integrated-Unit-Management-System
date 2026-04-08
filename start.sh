#!/usr/bin/env bash
# ================================================
#  В/Ч А7020 — Запуск сервера (Linux / macOS / WSL)
#  Версія 2.3
# ================================================

set -e

echo "========================================"
echo "   В/Ч А7020  —  Запуск сервера"
echo "========================================"

# Перевіряємо Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 не знайдено. Встановіть python3."
    exit 1
fi

# Перевіряємо, чи є server.py
if [ ! -f "server.py" ]; then
    echo "❌ server.py не знайдено у поточній папці!"
    exit 1
fi

# Параметри
NETWORK_MODE=false
if [[ "$1" == "--network" || "$1" == "-n" ]]; then
    NETWORK_MODE=true
    echo "🌐 Запуск у мережевому режимі (0.0.0.0)"
else
    echo "🔒 Запуск у локальному режимі (127.0.0.1)"
fi

# Створюємо папки
mkdir -p output static backups

# Запускаємо сервер
echo "🚀 Запускаємо сервер на порту 7020..."

if [ "$NETWORK_MODE" = true ]; then
    python3 server.py --network &
else
    python3 server.py &
fi

SERVER_PID=$!

# Чекаємо 2 секунди, щоб сервер стартував
sleep 2

# Відкриваємо браузер
echo "🌍 Відкриваю браузер..."
if command -v xdg-open &> /dev/null; then
    xdg-open "http://localhost:7020"
elif command -v open &> /dev/null; then
    open "http://localhost:7020"
else
    echo "Відкрийте вручну: http://localhost:7020"
fi

echo ""
echo "✅ Сервер запущено!"
echo "   Локально:   http://localhost:7020"
if [ "$NETWORK_MODE" = true ]; then
    echo "   Мережа:     http://$(hostname -I | awk '{print $1}'):7020"
fi
echo ""
echo "   Для зупинки натисніть Ctrl+C"
echo ""

# Тримаємо скрипт живим
wait $SERVER_PID