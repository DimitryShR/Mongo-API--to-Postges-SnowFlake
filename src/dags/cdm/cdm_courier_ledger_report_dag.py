import pendulum
from airflow.decorators import dag, task
from config_const import ConfigConst
from repositories.pg_connect import ConnectionBuilder

from cdm.courier_ledger_report import CouriersLedgerReportLoader



@dag(
    schedule_interval='0/30 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'cdm', 'courier_ledger'],
    is_paused_upon_creation=True
)
def sprint5_case_cdm_courier_ledger_report():
    @task
    def courier_ledger_daily_report_load():
        dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)
        rest_loader = CouriersLedgerReportLoader(dwh_pg_connect)
        rest_loader.load_couriers_ledger_report_by_days()

    courier_ledger_daily_report_load()  # type: ignore


my_dag = sprint5_case_cdm_courier_ledger_report()  # noqa
