from datetime import date, datetime, timedelta
from typing import Dict, List
from logging import Logger
import requests

from stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib.api_connect import APIConnect, APIHeaders
from lib.pg_connect import PgConnect
from stg.api.pg_api_saver import PgSaver
from lib.dict_util import json2str

class DeliveriesReader:
    def __init__(self, api_conn: APIConnect, api_header: APIHeaders) -> None:
        self.url = api_conn.get_url()
        self.header = api_header.get_headers()

    def get_deliveries(self, offset:int, datestart:datetime) -> List[Dict]:
        url_offset = '{url_}&offset={offset_}&from={from_}'.format(
            url_ = self.url,
            offset_ = offset,
            from_ = datestart
        )       
        # Вычитываем данные из API
        response = requests.get(url_offset, headers=self.header)
        response.raise_for_status()
        data_response = response.json()

        # print(data_response)
        
        return data_response


class DeliveriesLoader:
    _LOG_THRESHOLD = 2
    _OFFSET_THRESHOLD = 50
    WF_KEY = "api_deliveries_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, collection_loader: DeliveriesReader, pg_dest: PgConnect, pg_saver: PgSaver, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_dest = pg_dest
        self.pg_saver = pg_saver
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:
            
            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        # JSON ничего не знает про даты. Поэтому записываем строку, которую будем кастить при использовании.
                        # А в БД мы сохраним именно JSON.
                        self.LAST_LOADED_TS_KEY: str(datetime.combine(date.today() - timedelta(days=7), datetime.min.time()))
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str).isoformat(' ', timespec='seconds')
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")


            # update_ts = datetime.today() #Формируем единую дату в рамках транзакции
            offset = 0
            load_queue = []
            while True:
                curr_load_queue = self.collection_loader.get_deliveries(offset, last_loaded_ts)
                if len(curr_load_queue) > 0:
                    load_queue.extend(curr_load_queue)
                    curr_load_queue = []
                    offset += self._OFFSET_THRESHOLD
                else:
                    break
                
            self.log.info(f"Found {len(load_queue)} documents to sync from deliveries collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_deliveries_object(conn, str(d["delivery_id"]), str(d["delivery_ts"]), d)

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing deliveries.")

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t["delivery_ts"] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)
