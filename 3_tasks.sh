# Для выполнение задач использован PostgreSQL

# Задача 1
#!/bin/bash

# Условные параметры для подключения к базе данных PostgreSQL
DB_USER="user"
DB_PASSWORD="password"
DB_HOST="host"
DB_NAME="db_name"

# 1.1 Пересоздаём таблицу events
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "DROP TABLE IF EXISTS events;"
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "CREATE TABLE events (
    user_id varchar,
    product_identifier varchar,
    start_time timestamp,
    end_time timestamp,
    price_in_usd float
);"

# 1.2 Выгружаем CSV-файл
echo "user_id,product_identifier,start_time,end_time,price_in_usd" > events.csv
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -t -A -F ',' -c "COPY events TO STDOUT;" \
| awk -F',' -v OFS=',' '{gsub(/ /,"T",$3); gsub(/ /,"T",$4); print $0}' >> events.csv

# 1.3 Вставляем данные из CSV-файла в нашу таблицу
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "\COPY events FROM 'events.csv' CSV HEADER;"
# Удалим временный CSV-файл
rm events.csv

# Обеспечиваем исполняемость скрипта (при необходимости)
chmod +x creating_events.sh

# Задача 2

# 2.1 Создаём витрину данных на основе таблиц deposits и withdrawals
CREATE MATERIALIZED VIEW financial_snapshot AS
SELECT
  date_trunc('day', COALESCE(d.created_at, w.created_at)) AS transaction_date,
  COALESCE(SUM(d.amount), 0) AS total_deposits,
  COALESCE(SUM(w.amount), 0) AS total_withdrawals,
  COALESCE(SUM(d.amount), 0) - COALESCE(SUM(w.amount), 0) AS balance
FROM
  deposits d
FULL JOIN withdrawals w ON date_trunc('day', d.created_at) = date_trunc('day', w.created_at) AND d.user_id = w.user_id
GROUP BY
  transaction_date
ORDER BY
  transaction_date;

# 2.2 Варианты обновления витрины
# 2.2.1 Обновляем с cron (расписание -- каждый день в 00:00)
CREATE EXTENSION IF NOT EXISTS pg_cron;
# Создаём ежеденевное обновление
SELECT cron.schedule('0 0 * * *', $$REFRESH MATERIALIZED VIEW financial_snapshot;$$);
# Включаем планировщик, если он не был включен
SELECT cron.enable_cron();

# Или можно сохранить скрипт для обновления витрины
nano refresh_finansial_snapshot.sh
# Содержимое файла refresh_finansial_snapshot.sh
DB_USER="user"
DB_PASSWORD="password"
DB_HOST="host"
DB_NAME="db_name"
MATERIALIZED_VIEW="financial_snapshot"

psql -h $HOST -d $DATABASE -U $USER -c "REFRESH MATERIALIZED VIEW $MATERIALIZED_VIEW;"

# Предоставляем права на исполнение (при необходимости)
chmod +x refresh_finansial_snapshot.sh

# 2.2.2 Обновляем с помощью сторонних инстурментов (Python+Airflow)
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

HOST = "my_host"
DATABASE = "my_database"
USER = "my_user"
MATERIALIZED_VIEW = "financial_snapshot"

default_args = {
    'owner': 'owner_name',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('daily_update_financial_snapshot',
  default_args=default_args,
  schedule_interval='@daily') as dag:

  # Define the operator
  update_operator = BashOperator( 
    task_id='refresh_financial_snapshot',
    bash_command=f"./refresh_financial_snapshot.sh {HOST} {DATABASE} {USER} {MATERIALIZED_VIEW}",)
# Если задач в DAG несколько, можно приотизировать при помощи >>

# Задача 3

#!/bin/bash
# 3.1  Загружаем данные из csv в PostgreSQL DB
# Создадим временную таблицу
psql -c "CREATE TEMP TABLE temp_errors (id INT, date DATE, station VARCHAR(1), msg VARCHAR);" -U username -d database
# Загрузим данные из CSV во временную таблицу
psql -c "\COPY temp_errors FROM '/path/error_file.csv' DELIMITER ',' CSV HEADER;" -U username -d database

# 3.2 Создаём station_errors и присваиваем статусы ошибок
# 3.2.1 Создаём временную таблицу с предыдущим статусом
CREATE TEMP TABLE temp_errors_with_status AS
SELECT
  id,
  date,
  station,
  msg,
  LAG(msg) OVER (PARTITION BY station ORDER BY date) AS prev_msg,
  LAG(date) OVER (PARTITION BY station ORDER BY date) AS prev_date
FROM temp_errors;

# 3.2.2 Создаём функцию для определения статуса
CREATE OR REPLACE FUNCTION determine_status(current_msg VARCHAR, prev_msg VARCHAR, current_date DATE, prev_date DATE)
RETURNS VARCHAR
AS $$
BEGIN
  IF prev_msg IS NULL OR current_date - prev_date > '1 day' THEN
    RETURN 'new';
  ELSIF prev_msg = 'not finished' AND current_msg = 'not finished' THEN
    RETURN 'new';
  ELSIF prev_msg = 'fail' AND current_msg = 'not finished' THEN
    RETURN 'new';
  ELSIF prev_msg = 'fail' AND current_msg = 'fail' AND current_date - prev_date = '1 day' THEN
    RETURN 'serious';
  ELSIF prev_msg = 'serious' AND current_msg = 'fail' AND current_date - prev_date = '1 day' THEN
    RETURN 'critical';
  ELSIF prev_msg = 'critical' AND current_msg = 'fail' AND current_date - prev_date = '1 day' THEN
    RETURN 'critical';
  ELSE
    RETURN 'new';
  END IF;
END;
$$ LANGUAGE plpgsql;

# 3.2.3 Обновляем временную таблицу с добавлением статуса
UPDATE temp_errors_with_status
SET status = determine_status(msg, prev_msg, date, prev_date);
# Создаём таблицу station_errors
CREATE TABLE station_errors AS
SELECT id, date, station, msg, status FROM temp_errors_with_status;

