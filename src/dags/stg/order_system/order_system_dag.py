import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from config_const import ConfigConst
from stg.order_system.pg_saver import PgSaver
from stg.order_system.restaurant_loader import RestaurantLoader
from stg.order_system.restaurant_reader import RestaurantReader
from stg.order_system.user_loader import UserLoader
from stg.order_system.user_reader import UserReader
from stg.order_system.order_loader import OrderLoader
from stg.order_system.order_reader import OrderReader
from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'order_system'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_stg_order_system():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Получаем переменные из Airflow.
    cert_path = Variable.get(ConfigConst.MONGO_DB_CERTIFICATE_PATH)
    db_user = Variable.get(ConfigConst.MONGO_DB_USER)
    db_pw = Variable.get(ConfigConst.MONGO_DB_PASSWORD)
    rs = Variable.get(ConfigConst.MONGO_DB_REPLICA_SET)
    db = Variable.get(ConfigConst.MONGO_DB_DATABASE_NAME)
    host = Variable.get(ConfigConst.MONGO_DB_HOST)

    @task(task_id="restaurant_load")
    def load_restaurants():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем подключение r MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = RestaurantReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()
   
    @task(task_id="user_load")
    def load_users():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = UserReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = UserLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    @task(task_id="order_load")
    def load_orders():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = OrderReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = OrderLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()


    restaurant_loader = load_restaurants()
    user_loader = load_users()
    order_loader = load_orders()


    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    restaurant_loader >> user_loader >> order_loader # type: ignore


order_stg_dag = sprint5_stg_order_system()  # noqa
