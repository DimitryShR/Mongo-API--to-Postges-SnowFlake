from repositories.pg_connect import PgConnect


class CouriersLedgerRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_couriers_ledger_by_days(self) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
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
                    """
                )
                conn.commit()


class CouriersLedgerReportLoader:

    def __init__(self, pg: PgConnect) -> None:
        self.repository = CouriersLedgerRepository(pg)

    def load_couriers_ledger_report_by_days(self):
        self.repository.load_couriers_ledger_by_days()
