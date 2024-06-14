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
ADD CONSTRAINT dm_settlement_report_settlement_date_check CHECK (EXTRACT(YEAR FROM settlement_date) >= 2022 AND EXTRACT(YEAR FROM settlement_date) < 2050);

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