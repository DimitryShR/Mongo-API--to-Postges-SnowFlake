--CREATE SCHEMA dds;

DROP TABLE IF EXISTS dds.dm_users CASCADE;
CREATE TABLE dds.dm_users (
	id serial NOT NULL PRIMARY KEY,
	user_id varchar NOT NULL,
	user_name varchar NOT NULL,
	user_login varchar NOT NULL
);

DROP TABLE IF EXISTS dds.dm_restaurants CASCADE;
CREATE TABLE dds.dm_restaurants (
	id serial NOT NULL PRIMARY KEY,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL
);

DROP TABLE IF EXISTS dds.dm_products CASCADE;
CREATE TABLE dds.dm_products (
	id serial NOT NULL PRIMARY KEY,
	restaurant_id int4 NOT NULL,
	product_id varchar NOT NULL,
	product_name varchar NOT NULL,
	product_price NUMERIC (14,2) NOT NULL DEFAULT 0 constraint dm_products_product_price_check check (product_price >= 0),
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL
);

ALTER TABLE dds.dm_products DROP CONSTRAINT IF EXISTS dm_products_restaurant_id_fkey;
ALTER TABLE dds.dm_products ADD CONSTRAINT dm_products_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id);

DROP TABLE IF EXISTS dds.dm_timestamps CASCADE;
CREATE TABLE dds.dm_timestamps (
	id serial4 NOT NULL PRIMARY KEY,
	ts timestamp NOT NULL,
	"year" SMALLINT NOT NULL CONSTRAINT dm_timestamps_year_check CHECK ("year" >= 2022 AND "year" < 2500),
	"month" SMALLINT NOT NULL CONSTRAINT dm_timestamps_month_check CHECK ("month" >= 1 AND "month" <= 12),
	"day" SMALLINT NOT NULL CONSTRAINT dm_timestamps_day_check CHECK ("day" >= 1 AND "day" <= 31),
	"time" time NOT NULL,
	"date" date NOT NULL
);

DROP TABLE IF EXISTS dds.dm_orders CASCADE;
CREATE TABLE dds.dm_orders (
	id serial4 NOT NULL PRIMARY KEY,
	user_id int4 NOT NULL,
	restaurant_id int4 NOT NULL,
	timestamp_id int4 NOT NULL,
	order_key varchar NOT NULL,
	order_status varchar NOT NULL
);

ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES dds.dm_users (id);
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants (id);
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_timestamp_id_fkey FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps (id);

DROP TABLE IF EXISTS dds.fct_product_sales CASCADE;
CREATE TABLE dds.fct_product_sales (
	id serial4 NOT NULL PRIMARY KEY,
	product_id int4 NOT NULL,
	order_id int4 NOT NULL,
	"count" int4 NOT NULL DEFAULT 0 CONSTRAINT fct_product_sales_count_check CHECK ("count" >= 0),
	price numeric(14, 2) NOT NULL DEFAULT 0 CONSTRAINT fct_product_sales_price_check CHECK (price >= 0),
	total_sum numeric(14, 2) NOT NULL DEFAULT 0 CONSTRAINT fct_product_sales_total_sum_check CHECK (total_sum >= 0),
	bonus_payment numeric(14, 2) NOT NULL DEFAULT 0 CONSTRAINT fct_product_sales_bonus_payment_check CHECK (bonus_payment >= 0),
	bonus_grant numeric(14, 2) NOT NULL DEFAULT 0 CONSTRAINT fct_product_sales_bonus_grant_check CHECK (bonus_grant >= 0)
);

ALTER TABLE dds.fct_product_sales ADD CONSTRAINT fct_product_sales_product_id_fkey FOREIGN KEY (product_id) REFERENCES dds.dm_products (id);
ALTER TABLE dds.fct_product_sales ADD CONSTRAINT dm_orders_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders (id);

DROP TABLE IF EXISTS dds.srv_wf_settings CASCADE;
CREATE TABLE dds.srv_wf_settings (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id),
	CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);

DROP TABLE IF EXISTS dds.dm_couriers CASCADE;
CREATE TABLE dds.dm_couriers (
	id serial4 NOT NULL PRIMARY KEY,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL
	);
	
DROP TABLE IF EXISTS dds.dm_deliveries;
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