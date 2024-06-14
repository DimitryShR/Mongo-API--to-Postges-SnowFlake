DROP TABLE IF EXISTS cdm.dm_settlement_report;

CREATE TABLE cdm.dm_settlement_report (
	id serial NOT NULL,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	settlement_date date NOT NULL,
	orders_count integer NOT NULL,
	orders_total_sum numeric(14, 2) NOT NULL,
	orders_bonus_payment_sum numeric(14, 2) NOT NULL,
	orders_bonus_granted_sum numeric(14, 2) NOT NULL,
	order_processing_fee numeric(14, 2) NOT NULL,
	restaurant_reward_sum numeric(14, 2) NOT NULL
);

ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT pkey_id PRIMARY KEY (id);

ALTER TABLE cdm.dm_settlement_report
DROP CONSTRAINT IF EXISTS dm_settlement_report_settlement_date_check;

ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT dm_settlement_report_settlement_date_check CHECK (settlement_date >= '2022-01-01' AND settlement_date < '2500-01-01');

ALTER TABLE cdm.dm_settlement_report
ALTER COLUMN orders_count SET DEFAULT(0),
ALTER COLUMN orders_total_sum SET DEFAULT(0),
ALTER COLUMN orders_bonus_payment_sum SET DEFAULT(0),
ALTER COLUMN orders_bonus_granted_sum SET DEFAULT(0),
ALTER COLUMN order_processing_fee SET DEFAULT(0),
ALTER COLUMN restaurant_reward_sum SET DEFAULT(0),
ADD CONSTRAINT dm_settlement_report_orders_count_check CHECK (orders_count >= 0),
ADD CONSTRAINT dm_settlement_report_orders_total_sum_check CHECK (orders_total_sum >= 0),
ADD CONSTRAINT dm_settlement_report_orders_bonus_payment_sum_check CHECK (orders_bonus_payment_sum >= 0),
ADD CONSTRAINT dm_settlement_report_orders_bonus_granted_sum_check CHECK (orders_bonus_granted_sum >= 0),
ADD CONSTRAINT dm_settlement_report_order_processing_fee_check CHECK (order_processing_fee >= 0),
ADD CONSTRAINT dm_settlement_report_order_restaurant_reward_sum_check CHECK (restaurant_reward_sum >= 0);

ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT unique_index_restaurant_id_settlement_date UNIQUE(restaurant_id, settlement_date);



DROP TABLE IF EXISTS stg.bonussystem_users;
CREATE TABLE stg.bonussystem_users (
	id int4 NOT NULL,
	order_user_id text NOT NULL,
	CONSTRAINT users_pkey PRIMARY KEY (id)
);

DROP TABLE IF EXISTS stg.bonussystem_ranks;
CREATE TABLE stg.bonussystem_ranks (
	id int4 NOT NULL,
	"name" varchar(2048) NOT NULL,
	bonus_percent numeric(19, 5) NOT NULL DEFAULT 0,
	min_payment_threshold numeric(19, 5) NOT NULL DEFAULT 0,
	CONSTRAINT ranks_bonus_percent_check CHECK ((bonus_percent >= (0)::numeric)),
	CONSTRAINT ranks_bonus_percent_check1 CHECK ((bonus_percent >= (0)::numeric)),
	CONSTRAINT ranks_pkey PRIMARY KEY (id)
);

DROP TABLE IF EXISTS stg.bonussystem_events;
CREATE TABLE stg.bonussystem_events (
	id int4 NOT NULL,
	event_ts timestamp NOT NULL,
	event_type varchar NOT NULL,
	event_value text NOT NULL,
	CONSTRAINT outbox_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_bonussystem_events__event_ts ON stg.bonussystem_events USING btree (event_ts);


DROP TABLE IF EXISTS stg.ordersystem_orders;
CREATE TABLE stg.ordersystem_orders (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_orders_pkey PRIMARY KEY (id)
);

DROP TABLE IF EXISTS stg.ordersystem_restaurants;
CREATE TABLE stg.ordersystem_restaurants (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_restaurants_pkey PRIMARY KEY (id)
);

DROP TABLE IF EXISTS stg.ordersystem_users;
CREATE TABLE stg.ordersystem_users (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_users_pkey PRIMARY KEY (id)
);

ALTER TABLE stg.ordersystem_orders ADD CONSTRAINT ordersystem_orders_object_id_uindex UNIQUE (object_id);
ALTER TABLE stg.ordersystem_restaurants ADD CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE (object_id);
ALTER TABLE stg.ordersystem_users ADD CONSTRAINT ordersystem_users_object_id_uindex UNIQUE (object_id);

DROP TABLE IF EXISTS stg.srv_wf_settings;
CREATE TABLE stg.srv_wf_settings (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id),
	CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);

DROP TABLE IF EXISTS stg.api_couriers;
CREATE TABLE stg.api_couriers (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT api_couriers_object_id_uindex UNIQUE (object_id),
	CONSTRAINT api_couriers_pkey PRIMARY KEY (id)
);

DROP TABLE IF EXISTS stg.api_restaurants;
CREATE TABLE stg.api_restaurants (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT api_restaurants_object_id_uindex UNIQUE (object_id),
	CONSTRAINT api_restaurants_pkey PRIMARY KEY (id)
);

DROP TABLE IF EXISTS stg.api_deliveries;
CREATE TABLE stg.api_deliveries (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT api_deliveries_object_id_uindex UNIQUE (object_id),
	CONSTRAINT api_deliveries_pkey PRIMARY KEY (id)
);