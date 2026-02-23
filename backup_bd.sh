#!/bin/bash

# --- Конфигурация ---
DB_NAME="airflow"
DB_USER="airflow"
BACKUP_DIR="/var/lib/postgresql/backups"
KEEP_DAYS=7

# Дата в названии файла
DATE=$(date +%Y-%m-%d_%H-%M-%S)
BACKUP_FILE="$BACKUP_DIR/$DB_NAME-$DATE.sql.gz"

# Создание директории, если она не существует
mkdir -p "$BACKUP_DIR"

# --- Создание бэкапа ---
echo "Начало бэкапа: $DB_NAME"
pg_dump -U "$DB_USER" "$DB_NAME" | gzip > "$BACKUP_FILE"

if [ $? -eq 0 ]; then
  echo "Бэкап успешно создан: $BACKUP_FILE"
else
  echo "Ошибка при создании бэкапа"
  exit 1
fi

# --- Удаление старых бэкапов (старше 7 дней) ---
find "$BACKUP_DIR" -type f -name "*.sql.gz" -mtime +$KEEP_DAYS -delete
echo "Старые бэкапы удалены."
