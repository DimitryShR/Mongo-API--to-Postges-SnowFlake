import logging
import requests
import pendulum
from airflow.decorators import dag, task
# from config_const import ConfigConst
from stg.api.couriers_loader import CouriersReader, CouriersLoader
from stg.api.restaurants_loader import RestaurantsReader, RestaurantsLoader
from stg.api.deliveries_loader import DeliveriesReader, DeliveriesLoader
from airflow.hooks.http_hook import HttpHook
from lib.pg_connect import ConnectionBuilder
from lib.api_connect import APIConnect, APIHeaders
from stg.api.pg_api_saver import PgSaver
log = logging.getLogger(__name__)

http_conn_id = HttpHook.get_connection('api_conn')
api_key = http_conn_id.extra_dejson.get('api_key')
host = http_conn_id.host

NICKNAME = 'dimitry_sharov'
COHORT = '19'
# api_key = '25c27781-8fde-4b30-a22e-524044a7580f'

dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'api'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_stg_api_dag():

    @task(task_id="load_couriers")
    def load_couriers():

        file_name = 'couriers'
        sort_field = '_id'
        sort_direction = 'asc'
        limit = 50
        # offset = 0
        
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        api = APIConnect(host, file_name, sort_field, sort_direction, limit)
        headers = APIHeaders(NICKNAME, COHORT, api_key)
        
        couriers_reader = CouriersReader(api, headers)

        couriers_loader = CouriersLoader(couriers_reader, dwh_pg_connect, pg_saver, log)

        couriers_loader.run_copy()


    @task(task_id="load_restaurants")
    def load_restaurants():

        file_name = 'restaurants'
        sort_field = '_id'
        sort_direction = 'asc'
        limit = 50
        # offset = 0
        
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        api = APIConnect(host, file_name, sort_field, sort_direction, limit)
        headers = APIHeaders(NICKNAME, COHORT, api_key)
        
        restaurants_reader = RestaurantsReader(api, headers)

        couriers_loader = RestaurantsLoader(restaurants_reader, dwh_pg_connect, pg_saver, log)

        couriers_loader.run_copy()

    @task(task_id="load_deliveries")
    def load_deliveries():

        file_name = 'deliveries'
        sort_field = 'delivery_id'
        sort_direction = 'asc'
        limit = 50
        # offset = 0
        
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        api = APIConnect(host, file_name, sort_field, sort_direction, limit)
        headers = APIHeaders(NICKNAME, COHORT, api_key)
        
        restaurants_reader = DeliveriesReader(api, headers)

        couriers_loader = DeliveriesLoader(restaurants_reader, dwh_pg_connect, pg_saver, log)

        couriers_loader.run_copy()

    # Инициализируем объявленные таски.
    load_couriers = load_couriers()
    load_restaurants = load_restaurants()
    load_deliveries = load_deliveries()


    # Задаем последовательность выполнения тасков.
    load_couriers >> load_restaurants >> load_deliveries
    # >> users_dict >> outbox_dict


stg_bonus_system_ranks_dag = sprint5_stg_api_dag()
