import json
from datetime import datetime
from typing import List, Optional

from psycopg.rows import class_row
from pydantic import BaseModel
from repositories.pg_connect import PgConnect

from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting


class CourierJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime


class CourierDdsObj(BaseModel):
    id: int
    courier_id: str
    courier_name: str
    active_from: datetime
    active_to: datetime


class CourierRawRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_raw_couriers(self, last_loaded_record_id: int) -> List[CourierJsonObj]:
        with self._db.client().cursor(row_factory=class_row(CourierJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value,
                        update_ts
                    FROM stg.api_couriers
                    WHERE id > %(last_loaded_record_id)s;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs


class CourierDdsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_courier(self, courier: CourierDdsObj) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.dm_couriers(courier_id, courier_name, active_from, active_to)
                        VALUES (%(courier_id)s, %(courier_name)s, %(active_from)s, %(active_to)s);
                    """,
                    {
                        "courier_id": courier.courier_id,
                        "courier_name": courier.courier_name,
                        "active_from": courier.active_from,
                        "active_to": courier.active_to
                    },
                )
                conn.commit()

    def get_courier(self, courier_id: str) -> Optional[CourierDdsObj]:
        with self._db.client().cursor(row_factory=class_row(CourierDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        courier_id,
                        courier_name,
                        active_from,
                        active_to
                    FROM dds.dm_couriers
                    WHERE courier_id = %(courier_id)s;
                """,
                {"courier_id": courier_id},
            )
            obj = cur.fetchone()
        return obj


class CourierLoader:
    WF_KEY = "couriers_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.raw = CourierRawRepository(pg)
        self.dds = CourierDdsRepository(pg)
        self.settings_repository = settings_repository

    def parse_couriers(self, raws: List[CourierJsonObj]) -> List[CourierDdsObj]:
        res = []
        for r in raws:
            rest_json = json.loads(r.object_value)
            t = CourierDdsObj(id=r.id,
                                 courier_id=rest_json['_id'],
                                 courier_name=rest_json['name'],
                                #  active_from=datetime.strptime(str(r.update_ts), "%Y-%m-%d %H:%M:%S"),
                                 active_from=datetime.fromisoformat(str(r.update_ts)).isoformat(' ', timespec='seconds'),
                                #  active_from=datetime.strptime(rest_json['update_ts'], "%Y-%m-%d %H:%M:%S"),
                                 active_to=datetime(year=2099, month=12, day=31)
                                 )

            res.append(t)
        return res

    def load_couriers(self):
        wf_setting = self.settings_repository.get_setting(self.WF_KEY)
        if not wf_setting:
            wf_setting = EtlSetting(self.WF_KEY, {self.LAST_LOADED_ID_KEY: -1})

        last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

        load_queue = self.raw.load_raw_couriers(last_loaded_id)
        couriers_to_load = self.parse_couriers(load_queue)
        for r in couriers_to_load:
            self.dds.insert_courier(r)
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(
                r.id, wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY])

        self.settings_repository.save_setting(wf_setting)
