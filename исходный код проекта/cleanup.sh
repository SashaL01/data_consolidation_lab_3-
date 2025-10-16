#!/bin/bash

# Скрипт полной очистки проекта лабораторной работы №3
# Apache Airflow ETL Pipeline

echo "🧹 ПОЛНАЯ ОЧИСТКА DOCKER ОКРУЖЕНИЯ"
echo "⚠️  ВНИМАНИЕ: Будут удалены ВСЕ контейнеры, образы, тома и сети!"
echo ""

# Запрос подтверждения
read -p "Вы уверены, что хотите продолжить? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Очистка отменена пользователем"
    exit 1
fi

echo "🔄 Начинаем полную очистку Docker..."
echo ""

# 1. Остановка ВСЕХ контейнеров
echo "1️⃣  Остановка всех запущенных контейнеров..."
running_containers=$(sudo docker ps -q)
if [ ! -z "$running_containers" ]; then
    sudo docker stop $running_containers
    echo "✅ Остановлены все запущенные контейнеры"
else
    echo "ℹ️  Запущенные контейнеры не найдены"
fi
echo ""

# 2. Удаление контейнеров проекта
echo "2️⃣  Удаление контейнеров проекта..."
if sudo docker compose down -v --remove-orphans 2>/dev/null; then
    echo "✅ Контейнеры проекта удалены"
else
    echo "ℹ️  Docker compose файл не найден или контейнеры уже удалены"
fi
echo ""

# 3. Удаление ВСЕХ контейнеров
echo "3️⃣  Удаление всех контейнеров..."
all_containers=$(sudo docker ps -a -q)
if [ ! -z "$all_containers" ]; then
    sudo docker rm -f $all_containers
    echo "✅ Удалены все контейнеры"
else
    echo "ℹ️  Контейнеры не найдены"
fi
echo ""

# 4. Удаление образов проекта
echo "4️⃣  Удаление образов проекта..."
project_images=(
    "apache/airflow:2.5.0-python3.8"
    "postgres:12-alpine"
    "mailhog/mailhog:latest"
    "airflow-custom:latest"
)

for image in "${project_images[@]}"; do
    if sudo docker rmi -f "$image" 2>/dev/null; then
        echo "✅ Удален образ: $image"
    else
        echo "ℹ️  Образ не найден: $image"
    fi
done
echo ""

# 5. Удаление ВСЕХ неиспользуемых образов
echo "5️⃣  Удаление всех неиспользуемых образов..."
if sudo docker image prune -a -f; then
    echo "✅ Удалены все неиспользуемые образы"
else
    echo "⚠️  Ошибка при удалении образов"
fi
echo ""

# 6. Удаление ВСЕХ томов
echo "6️⃣  Удаление всех томов..."
all_volumes=$(sudo docker volume ls -q)
if [ ! -z "$all_volumes" ]; then
    sudo docker volume rm -f $all_volumes 2>/dev/null
    echo "✅ Удалены все тома"
else
    echo "ℹ️  Тома не найдены"
fi
echo ""

# 7. Удаление ВСЕХ сетей (кроме системных)
echo "7️⃣  Удаление всех пользовательских сетей..."
if sudo docker network prune -f; then
    echo "✅ Удалены все пользовательские сети"
else
    echo "⚠️  Ошибка при удалении сетей"
fi
echo ""

# 8. Полная очистка системы Docker
echo "8️⃣  Полная очистка системы Docker..."
if sudo docker system prune -a -f --volumes; then
    echo "✅ Выполнена полная очистка системы Docker"
else
    echo "⚠️  Ошибка при полной очистке системы"
fi
echo ""

# 9. Удаление локальных файлов проекта
echo "9️⃣  Удаление локальных файлов проекта..."

# Удаление SQLite баз данных
rm -f *.db 2>/dev/null && echo "✅ Удалены файлы баз данных (*.db)" || echo "ℹ️  Файлы баз данных не найдены"

# Удаление логов
rm -rf logs/ 2>/dev/null && echo "✅ Удалена папка логов" || echo "ℹ️  Папка логов не найдена"

# Удаление временных файлов Python
find . -name "*.pyc" -delete 2>/dev/null && echo "✅ Удалены временные файлы Python (.pyc)"
find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null && echo "✅ Удалены кэш-папки Python (__pycache__)"

# Удаление других временных файлов
rm -f .DS_Store 2>/dev/null
rm -f Thumbs.db 2>/dev/null
echo ""

# 10. Проверка результатов очистки
echo "🔟 Проверка результатов очистки..."
echo ""

echo "📊 Статистика Docker после очистки:"
sudo docker system df
echo ""

echo "📋 Оставшиеся контейнеры:"
container_count=$(sudo docker ps -a -q | wc -l)
if [ "$container_count" -eq 0 ]; then
    echo "✅ Контейнеры отсутствуют"
else
    echo "⚠️  Найдены контейнеры:"
    sudo docker ps -a --format "table {{.Names}}\t{{.Image}}\t{{.Status}}"
fi
echo ""

echo "📋 Оставшиеся образы:"
image_count=$(sudo docker images -q | wc -l)
if [ "$image_count" -eq 0 ]; then
    echo "✅ Образы отсутствуют"
else
    echo "ℹ️  Найдены образы:"
    sudo docker images --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}"
fi
echo ""

echo "📋 Оставшиеся тома:"
volume_count=$(sudo docker volume ls -q | wc -l)
if [ "$volume_count" -eq 0 ]; then
    echo "✅ Тома отсутствуют"
else
    echo "⚠️  Найдены тома:"
    sudo docker volume ls
fi
echo ""

echo "📋 Оставшиеся сети:"
network_count=$(sudo docker network ls --filter type=custom -q | wc -l)
if [ "$network_count" -eq 0 ]; then
    echo "✅ Пользовательские сети отсутствуют"
else
    echo "ℹ️  Найдены пользовательские сети:"
    sudo docker network ls --filter type=custom
fi
echo ""

# 11. Финальное сообщение
echo "🎉 ПОЛНАЯ ОЧИСТКА ЗАВЕРШЕНА!"
echo ""
echo "📁 Структура проекта после очистки:"
ls -la
echo ""

echo "💾 Освобождено места на диске:"
sudo docker system df
echo ""

echo "🔄 Для повторного запуска проекта выполните:"
echo "   sudo docker compose up -d"
echo ""

echo "📚 Для полного удаления папки проекта выполните:"
echo "   cd .. && rm -rf $(basename $(pwd))/"
echo ""

echo "✨ Docker окружение полностью очищено!"
echo "🚀 Готово к новому запуску проекта!"
