import logging

import pendulum
from airflow.decorators import dag, task
from config_const import ConfigConst
from stg.bonus_system.ranks_loader import RankLoader
from stg.bonus_system.users_loader import UserLoader
from stg.bonus_system.outbox_loader import OutboxLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'bonus_system'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_stg_bonus_system_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_ORIGIN_BONUS_SYSTEM_CONNECTION)

    # Объявляем таск, который загружает данные.
    @task(task_id="ranks_load")
    def load_ranks():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_ranks()  # Вызываем функцию, которая перельет данные.



    @task(task_id="users_load")
    def load_users():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()  # Вызываем функцию, которая перельет данные.



    @task(task_id="outbox_load")
    def load_outbox():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = OutboxLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_outbox()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    ranks_dict = load_ranks()
    users_dict = load_users()
    outbox_dict = load_outbox()

    # Задаем последовательность выполнения тасков.
    ranks_dict >> users_dict >> outbox_dict


stg_bonus_system_ranks_dag = sprint5_stg_bonus_system_dag()
