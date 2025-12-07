import pandas as pd
import time
import db
import sqlite3 as sql
from producer import Event

RAW_CSV_PATH = "data/raw_events.csv"
CSV_HEADER = [
    "event_id",
    "timestamp",
    "order_status",
    "city_name",
    "order_id",
    "user_id",
    "restaurant_id",
    "courier_id",
    "order_value",
    "delivery_fee",
    "pickup_time",
    "delivery_time",
    "wait_time",
    "pickup_lat",
    "pickup_lon",
    "dropoff_lat",
    "dropoff_lon"]



#print(type(data['timestamp'][1]))


def run_etl():
    while True:
        data = pd.read_csv(RAW_CSV_PATH)

        max_event_id = db.get_max_event_id()
        new_data = data[data['event_id'] > max_event_id]


        if new_data.empty:
            return
        else:
            data = new_data
            for index, row in data.iterrows():
                ev = Event()
                ev.event_id = row['event_id']
                ev.timestamp = row['timestamp']
                ev.order_status = row['order_status']
                ev.city_name = row['city_name']
                ev.order_id = row['order_id']
                ev.user_id = row['user_id']
                ev.restaurant_id = row['restaurant_id']
                ev.courier_id = row['courier_id']
                ev.order_value = row['order_value']
                ev.delivery_fee = row['delivery_fee']
                ev.pickup_time = row['pickup_time']
                ev.delivery_time = row['delivery_time']
                ev.wait_time = row['wait_time']
                ev.pickup_lat = row['pickup_lat']
                ev.pickup_lon = row['pickup_lon']
                ev.dropoff_lat = row['dropoff_lat']
                ev.dropoff_lon = row['dropoff_lon']
                db.insert_event(ev)
        time.sleep(1)