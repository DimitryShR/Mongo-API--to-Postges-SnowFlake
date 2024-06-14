import json
from datetime import datetime

from delivery_repositories import (DeliveryDdsObj, DeliveryDdsRepository, DeliveryJsonObj, DeliveryRawRepository)
from repositories.pg_connect import PgConnect
from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from dds.couriers_loader import CourierDdsRepository
from dds.timestamp_loader import TimestampDdsRepository
from dds.order_loader import OrderDdsRepository


class DeliveryLoader:
    WF_KEY = "delivery_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.raw = DeliveryRawRepository(pg)
        self.dds_couriers = CourierDdsRepository(pg)
        self.dds_timestamps = TimestampDdsRepository(pg)
        self.dds_orders = OrderDdsRepository(pg)
        self.dds_deliveries = DeliveryDdsRepository(pg)
        self.settings_repository = settings_repository

    def parse_delivery(self, delivery_raw: DeliveryJsonObj, courier_id: int, order_id: int) -> DeliveryDdsObj:
        delivery_json = json.loads(delivery_raw.object_value)

        t = DeliveryDdsObj(id=0,
                        delivery_key=delivery_json['delivery_id'],
                        courier_id=courier_id,
                        delivery_ts=delivery_json['delivery_ts'],
                        order_id=order_id,
                        address=delivery_json['address'],
                        rate=delivery_json['rate'],
                        sum=delivery_json['sum'],
                        tip_sum=delivery_json['tip_sum']
                        )

        return t

    def load_deliveries(self):
        wf_setting = self.settings_repository.get_setting(self.WF_KEY)
        if not wf_setting:
            wf_setting = EtlSetting(self.WF_KEY, {self.LAST_LOADED_ID_KEY: -1})

        last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

        load_queue = self.raw.load_raw_deliveries(last_loaded_id)
        for delivery in load_queue:
            delivery_json = json.loads(delivery.object_value)

            courier = self.dds_couriers.get_courier(delivery_json['courier_id'])
            if not courier:
                continue

            order = self.dds_orders.get_order(delivery_json['order_id'])
            if not order:
                continue

            delivery_to_load = self.parse_delivery(delivery, courier.id, order.id)
            self.dds_deliveries.insert_delivery(delivery_to_load)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(
                delivery.id, wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY])

        self.settings_repository.save_setting(wf_setting)
