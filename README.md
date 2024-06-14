## Описание проекта
Проект по доставке данных из 3-х источников (MongoDB, PostgresSQL, API) в DWH (Postgres) с 3 слоями (stg, dds(Snowflake), cdm)
Инкрементальная загрузка данных с условием соблюдения идемпотентности
Оркестрация процессами AirFlow

## Что сделано
1. Спроектировано и реализовано DWH в Postgres:
- stg
- dds (Snowflake)
- cdm

2. Реализован пайплайн доставки данных из MongoDB, Postgres, API в stg слой DWH as is (PostgreSQL) средствами python

3. Реализован пайплайн загрузки данных из stg слоя, парсинга, маппинга и заливки в dds слой средствами python

4. Реализованы алгоритмы расчета витрин, пайплайн переливки данных из dds слоя в cmd средствами python

5. Реализованы 3 dag для заливки данных в stg из 3 источников, 1 dag для заливки данных в ddl, 2 dag для расчета и импорта данных в витрины