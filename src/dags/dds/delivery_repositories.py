from typing import List, Optional
from datetime import datetime

from psycopg.rows import class_row
from pydantic import BaseModel
from repositories.pg_connect import PgConnect

class DeliveryJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class DeliveryRawRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_raw_deliveries(self, last_loaded_record_id: int) -> List[DeliveryJsonObj]:
        with self._db.client().cursor(row_factory=class_row(DeliveryJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.api_deliveries
                    WHERE id > %(last_loaded_record_id)s;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs


class DeliveryDdsObj(BaseModel):
    id: int
    delivery_key: str
    courier_id: int
    delivery_ts: datetime
    order_id: int
    address: str
    rate: int
    sum: float
    tip_sum: float



class DeliveryDdsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_delivery(self, delivery: DeliveryDdsObj) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.dm_deliveries(delivery_key, courier_id, delivery_ts, order_id, address, rate, sum, tip_sum)
                        VALUES (%(delivery_key)s, %(courier_id)s, %(delivery_ts)s, %(order_id)s, %(address)s, %(rate)s, %(sum)s, %(tip_sum)s);
                    """,
                    {
                        "delivery_key": delivery.delivery_key,
                        "courier_id": delivery.courier_id,
                        "delivery_ts": delivery.delivery_ts,
                        "order_id": delivery.order_id,
                        "address": delivery.address,
                        "rate": delivery.rate,
                        "sum": delivery.sum,
                        "tip_sum": delivery.tip_sum
                    },
                )
                conn.commit()

    # def get_delivery(self, order_id: str) -> Optional[DeliveryDdsObj]:
    #     with self._db.client().cursor(row_factory=class_row(DeliveryDdsObj)) as cur:
    #         cur.execute(
    #             """
    #                 SELECT
    #                     id,
    #                     delivery_key,
    #                     courier_id,
    #                     delivery_ts,
    #                     order_id,
    #                     address,
    #                     rate,
    #                     sum,
    #                     tip_sum
    #                 FROM dds.dm_deliveries
    #                 WHERE order_key = %(order_id)s;
    #             """,
    #             {"order_id": order_id},
    #         )
    #         obj = cur.fetchone()
    #     return obj
