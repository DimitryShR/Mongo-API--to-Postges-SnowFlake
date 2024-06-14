from datetime import datetime
from typing import Any

from lib.dict_util import json2str
from psycopg import Connection


class PgSaver:

    def save_couriers_object(self, conn: Connection, id: str, update_ts: datetime, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.api_couriers(object_id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO NOTHING;
                """,
                {
                    "id": id,
                    "val": str_val,
                    "update_ts": update_ts
                }
            )

    def save_restaurants_object(self, conn: Connection, id: str, update_ts: datetime, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.api_restaurants(object_id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO NOTHING;
                """,
                {
                    "id": id,
                    "val": str_val,
                    "update_ts": update_ts
                }
            )

    def save_deliveries_object(self, conn: Connection, id: str, update_ts: datetime, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.api_deliveries(object_id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                """,
                {
                    "id": id,
                    "val": str_val,
                    "update_ts": update_ts
                }
            )