# 1. Cтруктура и алгоритм формирования витрины расчётов с курьерами

Витрина содержит информацию о выплатах курьерам.

### Состав витрины:

- id — идентификатор записи.
- courier_id — ID курьера, которому перечисляем.
- courier_name — Ф. И. О. курьера.
- settlement_year — год отчёта.
- settlement_month — месяц отчёта, где 1 — январь и 12 — декабрь.
- orders_count — количество заказов за период (месяц).
- orders_total_sum — общая стоимость заказов.
- rate_avg — средний рейтинг курьера по оценкам пользователей.
- order_processing_fee — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25.
- courier_order_sum — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).
- courier_tips_sum — сумма, которую пользователи оставили курьеру в качестве чаевых.
- courier_reward_sum — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).

### Правила расчёта процента выплаты курьеру

В зависимости от рейтинга, где r — это средний рейтинг курьера в расчётном месяце:
- r < 4 — 5% от заказа, но не менее 100 р.;
- 4 <= r < 4.5 — 7% от заказа, но не менее 150 р.;
- 4.5 <= r < 4.9 — 8% от заказа, но не менее 175 р.;
- 4.9 <= r — 10% от заказа, но не менее 200 р.

Данные о заказах уже есть в хранилище. Данные курьерской службы вам необходимо забрать из API курьерской службы, после чего совместить их с данными подсистемы заказов.

Отчёт собирается по дате заказа. Если заказ был сделан ночью и даты заказа и доставки не совпадают, в отчёте стоит ориентироваться на дату заказа, а не дату доставки. Иногда заказы, сделанные ночью до 23:59, доставляют на следующий день: дата заказа и доставки не совпадёт. Это важно, потому что такие случаи могут выпадать в том числе и на последний день месяца. Тогда начисление курьеру относите к дате заказа, а не доставки.

### DDL Витрины

```SQL
DROP TABLE IF EXISTS cdm.dm_courier_ledger;
CREATE TABLE cdm.dm_courier_ledger (
	id serial4 NOT NULL PRIMARY KEY,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year int2 NOT NULL,
	settlement_month int2 NOT NULL,
	orders_count int2 NOT NULL DEFAULT(0) CHECK (orders_count >= 0),
	orders_total_sum numeric(14,2) NOT NULL DEFAULT(0) CHECK (orders_total_sum >= 0),
	rate_avg numeric(14,2) NOT NULL DEFAULT(0) CHECK (rate_avg >= 0),
	order_processing_fee numeric(14,2) NOT NULL DEFAULT(0) CHECK (order_processing_fee >= 0),
	courier_order_sum numeric(14,2) NOT NULL DEFAULT(0) CHECK (courier_order_sum >= 0),
	courier_tips_sum numeric(14,2) NOT NULL DEFAULT(0) CHECK (courier_tips_sum >= 0),
	courier_reward_sum numeric(14,2) NOT NULL DEFAULT(0) CHECK (courier_reward_sum >= 0)
);

CREATE UNIQUE INDEX unique_index_courier_id_settlement_year_settlement_month ON cdm.dm_courier_ledger USING btree (courier_id, settlement_year, settlement_month);
```

### Алгоритм расчета витрины
```SQL
WITH intermediate_table AS (
	SELECT
		--dd.courier_id,
		dc.courier_id		AS courier_id,
		dc.courier_name	AS courier_name,
		dt."year"			AS settlement_year,
		dt."month"			AS settlement_month,
		dd.order_id,
		dd.rate,
		dd.sum,
		dd.tip_sum
	FROM dds.dm_deliveries dd
	LEFT JOIN dds.dm_couriers dc ON dc.id = dd.courier_id
	LEFT JOIN dds.dm_orders do2 ON do2.id = dd.order_id 
	LEFT JOIN dds.dm_timestamps dt ON dt.id = do2.timestamp_id
	),
avg_rate AS (
	SELECT
		courier_id,
		settlement_year,
		settlement_month,
		avg(rate)	AS avg_rate
	FROM intermediate_table
	GROUP BY 
		courier_id,
		settlement_year,
		settlement_month
	),
intermediate_table_2 AS (
	SELECT
--		it.courier_id,
		it.order_id	AS order_id,
--		it.sum,
--		ar.avg_rate,
		CASE
			WHEN ar.avg_rate<4 THEN (CASE WHEN it.sum * 0.05 <= 100 THEN 100 ELSE it.sum * 0.05 END)
			WHEN ar.avg_rate<4.5 THEN (CASE WHEN it.sum * 0.07 <= 150 THEN 150 ELSE it.sum * 0.07 END)
			WHEN ar.avg_rate<4.9 THEN (CASE WHEN it.sum * 0.08 <= 175 THEN 175 ELSE it.sum * 0.08 END)
			ELSE (CASE WHEN it.sum * 0.1 <= 200 THEN 200 ELSE it.sum * 0.1 END)
		END	AS courier_intermediate_order_sum
	FROM intermediate_table it
	LEFT JOIN avg_rate ar ON ar.courier_id = it.courier_id
	)
INSERT INTO cdm.dm_courier_ledger
(courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, 
order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
SELECT
	it.courier_id							AS courier_id,
	it.courier_name							AS courier_name,
	it.settlement_year						AS settlement_year,
	it.settlement_month						AS settlement_month,
	ar.avg_rate								AS rate_avg,
	count(it.order_id)						AS orders_count,
	sum(it."sum")							AS orders_total_sum,
	sum(it."sum") * 0.25 					AS order_processing_fee,
	sum(it2.courier_intermediate_order_sum)	AS courier_order_sum,
	sum(it.tip_sum)							AS courier_tips_sum,
	--( sum(it.tip_sum) + sum(it2.courier_intermediate_order_sum) ) * 0.95,
	trunc( ( sum(it.tip_sum) + sum(it2.courier_intermediate_order_sum) ) * 0.95, 2 )	AS courier_reward_sum
FROM intermediate_table it
LEFT JOIN intermediate_table_2 it2 ON it2.order_id = it.order_id
LEFT JOIN avg_rate ar ON ar.courier_id = it.courier_id
GROUP BY
	it.courier_id,
	it.courier_name,
	it.settlement_year,
	it.settlement_month,
	ar.avg_rate
ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
SET
	orders_count = EXCLUDED.orders_count,
	orders_total_sum = EXCLUDED.orders_total_sum,
	rate_avg = EXCLUDED.rate_avg,
	order_processing_fee = EXCLUDED.order_processing_fee,
	courier_order_sum = EXCLUDED.courier_order_sum,
	courier_tips_sum = EXCLUDED.courier_tips_sum,
	courier_reward_sum = EXCLUDED.courier_reward_sum;

```

# 2. Структура DDS слоя

```SQL
DROP TABLE IF EXISTS dds.fct_product_sales;
DROP TABLE IF EXISTS dds.srv_wf_settings;
DROP TABLE IF EXISTS dds.dm_orders;
DROP TABLE IF EXISTS dds.dm_products;
DROP TABLE IF EXISTS dds.dm_restaurants;
DROP TABLE IF EXISTS dds.dm_users;
DROP TABLE IF EXISTS dds.dm_timestamps;
DROP TABLE IF EXISTS dds.dm_couriers;
DROP TABLE IF EXISTS dds.dm_deliveries;

CREATE TABLE dds.srv_wf_settings(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    workflow_key varchar UNIQUE,
    workflow_settings text
);

CREATE TABLE dds.dm_restaurants(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    restaurant_id varchar NOT NULL,
    restaurant_name text NOT NULL,

    active_from timestamp NOT NULL,
    active_to timestamp NOT NULL
);

CREATE TABLE dds.dm_products (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    
    product_id varchar NOT NULL,
    product_name text NOT NULL,
    product_price numeric(19, 5) NOT NULL DEFAULT 0 CHECK (product_price >= 0),

    active_from timestamp NOT NULL,
    active_to timestamp NOT NULL,
    
    restaurant_id int NOT NULL REFERENCES dds.dm_restaurants(id)
);

CREATE TABLE dds.dm_timestamps(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    ts timestamp NOT NULL,

    year int NOT NULL CHECK(year >= 2020 AND year < 2500),
    month int NOT NULL CHECK(month >= 0 AND month <= 12),
    day int NOT NULL CHECK(day >= 0 AND day <= 31),
    time time NOT NULL,
    date date NOT NULL
);

CREATE TABLE dds.dm_users(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    user_id varchar NOT NULL,
    user_name varchar NOT NULL,
    user_login varchar NOT NULL
);

CREATE TABLE dds.dm_orders(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    order_key varchar NOT NULL,
    order_status varchar NOT NULL,
    
    restaurant_id int NOT NULL REFERENCES dds.dm_restaurants(id),
    timestamp_id int NOT NULL REFERENCES dds.dm_timestamps(id),
    user_id int NOT NULL REFERENCES dds.dm_users(id)
);


CREATE TABLE dds.fct_product_sales (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    product_id int NOT NULL REFERENCES dds.dm_products(id),
    order_id int NOT NULL REFERENCES dds.dm_orders(id),
    count int NOT NULL DEFAULT 0 CHECK (count >= 0),
    price numeric(19, 5) NOT NULL DEFAULT 0 CHECK (price >= 0),
    total_sum numeric(19, 5) NOT NULL DEFAULT 0 CHECK (total_sum >= 0),
    bonus_payment numeric(19, 5) NOT NULL DEFAULT 0 CHECK (bonus_payment >= 0),
    bonus_grant numeric(19, 5) NOT NULL DEFAULT 0 CHECK (bonus_grant >= 0)
);


CREATE TABLE dds.dm_couriers (
	id serial4 NOT NULL PRIMARY KEY,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL
	);

CREATE TABLE dds.dm_deliveries (
	id serial4 NOT NULL PRIMARY KEY,
	delivery_key varchar NOT NULL,
	courier_id int NOT NULL,
	delivery_ts timestamp NOT NULL,
	--order_key varchar NOT NULL,
	order_id int NOT NULL,
	--order_ts timestamp,
	address varchar NOT NULL,
	rate NUMERIC(14,2),
	"sum" NUMERIC(14,2),
	tip_sum NUMERIC(14,2),
	CONSTRAINT dm_deliveries_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id),
	CONSTRAINT dm_deliveries_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id)
	);
```

# 3. Структура stg слоя

Выгружаем все 3 таблицы `as is`. Храним содержание передаваемого объекта (json) в формате str, т.е. `as is`.

При переносе в DDS слой парсим json.

### Курьеры (stg.api_couriers)

В передаваемом json нет даты, но будем хранить дату выгрузки данных из api в поле update_ts.
Для данной таблицы не реализовано хранение курсора (последней выгруженной id в таблице srv_wf_settings), т.к. нет возможности установить данный курсор при запросе.
Устанавливать курсор по offset считаю не корректным, т.к. id не порядковый, а буквенно-численный ключ.

```sql
CREATE TABLE stg.api_couriers (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT api_couriers_object_id_uindex UNIQUE (object_id),
	CONSTRAINT api_couriers_pkey PRIMARY KEY (id)
);
```
### Доставки (stg.api_deliveries)

Берем дату update_ts из json (delivery_ts). По ней же и формируем курсор для инкрементальной загрузки данных.

```sql
CREATE TABLE stg.api_deliveries (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT api_deliveries_object_id_uindex UNIQUE (object_id),
	CONSTRAINT api_deliveries_pkey PRIMARY KEY (id)
);
```

### Рестораны (stg.api_restaurants)

Таблицы с ресторанами не востребована, но пусть лежит в stg.
update_ts так же как и в курьерах (дата выгрузги по api).
Также не реализовано хранение курсора (последней выгруженной id в таблице srv_wf_settings)

```sql
CREATE TABLE stg.api_restaurants (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT api_restaurants_object_id_uindex UNIQUE (object_id),
	CONSTRAINT api_restaurants_pkey PRIMARY KEY (id)
);
```