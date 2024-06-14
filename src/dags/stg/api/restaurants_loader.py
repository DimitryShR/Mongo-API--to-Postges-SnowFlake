from datetime import datetime
from typing import Dict, List
from logging import Logger
import requests

from lib.api_connect import APIConnect, APIHeaders
from lib.pg_connect import PgConnect
from stg.api.pg_api_saver import PgSaver

class RestaurantsReader:
    def __init__(self, api_conn: APIConnect, api_header: APIHeaders) -> None:
        self.url = api_conn.get_url()
        self.header = api_header.get_headers()

    def get_restaurants(self, offset:int) -> List[Dict]:
        url_offset = '{url_}&offset={offset_}'.format(
            url_ = self.url,
            offset_ = offset)
        
        # Вычитываем данные из API
        response = requests.get(url_offset, headers=self.header)
        response.raise_for_status()
        data_response = response.json()

        # print(data_response)
        
        return data_response


class RestaurantsLoader:
    _LOG_THRESHOLD = 2
    _OFFSET_THRESHOLD = 50

    def __init__(self, collection_loader: RestaurantsReader, pg_dest: PgConnect, pg_saver: PgSaver, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_dest = pg_dest
        self.pg_saver = pg_saver
        self.log = logger

    def run_copy(self) -> int:
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:
            
            update_ts = datetime.today() #Формируем единую дату в рамках транзакции
            offset = 0
            load_queue = []
            while True:
                curr_load_queue = self.collection_loader.get_restaurants(offset)
                if len(curr_load_queue) > 0:
                    load_queue.extend(curr_load_queue)
                    curr_load_queue = []
                    offset += self._OFFSET_THRESHOLD
                else:
                    break
                
            self.log.info(f"Found {len(load_queue)} documents to sync from restaurants collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_restaurants_object(conn, str(d["_id"]), update_ts, d)

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing restaurants.")

            self.log.info(f"Finishing work.")

            return len(load_queue)
