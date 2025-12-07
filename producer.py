# This is the producer module which will generate a data log in real time meant to simulate real data from a service such as foodora.
import time
import random
import csv
import os
from datetime import datetime
import enum

RAW_CSV_PATH = 'data/raw_events.csv'


class event_types(enum.Enum):
    NONE = "NONE"
    ORDER_PLACED = "ORDER_PLACED"
    ORDER_ACCEPTED = "ORDER_ACCEPTED"
    COURIER_ASSIGNED = "COURIER_ASSIGNED"
    ORDER_PREPARED = "ORDER_PREPARED"
    ORDER_PICKED_UP = "ORDER_PICKED_UP"
    ORDER_DELIVERED = "ORDER_DELIVERED"

    ORDER_CANCELLED_USR = "ORDER_CANCELLED_USR"
    ORDER_CANCELLED_REST = "ORDER_CANCELLED_REST"
    ORDER_CANCELLED_COUR = "ORDER_CANCELLED_COUR"
    PAYMENT_FAILED = "PAYMENT_FAILED"
    APPLICATION_ERROR = "APPLICATION_ERROR"
    COURIER_UNAVAILABLE = "COURIER_UNAVAILABLE"

    API_REQUEST = "API_REQUEST"
    API_FAILED = "API_FAILED"
    LATENCY_ISSUE = "LATENCY_ISSUE"

ORDER_STATE_TRANSITIONS = {
    event_types.ORDER_PLACED: [
        (event_types.ORDER_ACCEPTED, 0.8),
        (event_types.ORDER_CANCELLED_USR, 0.15),
        (event_types.ORDER_CANCELLED_REST, 0.05),
    ],
    event_types.ORDER_ACCEPTED: [
        (event_types.COURIER_ASSIGNED, 0.9),
        (event_types.ORDER_CANCELLED_REST, 0.1),
    ],
    event_types.COURIER_ASSIGNED: [
        (event_types.ORDER_PREPARED, 0.9),
        (event_types.ORDER_CANCELLED_COUR, 0.1),
    ],
    event_types.ORDER_PREPARED: [
        (event_types.ORDER_PICKED_UP, 0.95),
        (event_types.ORDER_CANCELLED_COUR, 0.05),
    ],
    event_types.ORDER_PICKED_UP: [
        (event_types.ORDER_DELIVERED, 1.0),
    ],
    # terminala states får tom lista
    event_types.ORDER_DELIVERED: [],
    event_types.ORDER_CANCELLED_USR: [],
    event_types.ORDER_CANCELLED_REST: [],
    event_types.ORDER_CANCELLED_COUR: [],
}

class city(enum.Enum):
    NONE = "NONE"
    STOCKHOLM = "Stockholm"
    MALMO = "Malmo"
    LINKOPING = "Linkoping"

stockholm_lat_range = (59.3030, 59.4500)
stockholm_lon_range = (18.0000, 18.1500)
stockholm_resturaunt_cord_list = [
    (784, 59.332580, 18.064900),
    (703, 59.342580, 18.054900), 
    (834, 59.352580, 18.074900), 
    (723, 59.362580, 18.084900), 
    (119, 59.372580, 18.094900),
    (100, 59.382580, 18.104900),
    (742, 59.392580, 18.114900),
    (951, 59.402580, 18.124900),
    (851, 59.412580, 18.134900),
    (990, 59.422580, 18.144900)
]

malmo_lat_range = (55.5300, 55.6200)
malmo_lon_range = (12.9500, 13.0500)
malmo_resturaunt_cord_list = [
    (894, 55.5900, 13.0000),
    (924, 55.5950, 13.0100),
    (904, 55.6000, 13.0200),
    (888, 55.6050, 13.0300),
    (163, 55.6100, 13.0400),
    (227, 55.6150, 13.0500),
    (884, 55.6200, 13.0600),
    (724, 55.6250, 13.0700),
    (824, 55.6300, 13.0800),
    (815, 55.6350, 13.0900)
]

linkoping_lat_range = (58.3500, 58.4500)
linkoping_lon_range = (15.5000, 15.6500)
linkoping_resturaunt_id_cord_list = [
    (531, 58.3800, 15.5500),
    (713, 58.3850, 15.5600),
    (184, 58.3900, 15.5700),
    (872, 58.3950, 15.5800),
    (726, 58.4000, 15.5900),
    (681, 58.4050, 15.6000),
    (117, 58.4100, 15.6100),
    (937, 58.4150, 15.6200),
    (961, 58.4200, 15.6300),
    (835, 58.4250, 15.6400)
]

user_id_list = random.sample(range(1000, 5001), 100)
courier_id_list = random.sample(range(5000, 10001), 50)


class Event:
    def __init__(self):
        self.event_id = -1
        self.timestamp = int(time.time())
        self.order_status = event_types.NONE
        self.city_name = city.NONE


        self.order_id = -1
        self.user_id = -1
        self.restaurant_id = -1
        self.courier_id = -1
        self.order_value = -1.0
        self.delivery_fee = -1.0
        self.pickup_time = int(time.time())
        self.delivery_time = int(time.time())
        self.wait_time = -1  # time between order placed and order picked up

        self.pickup_lon = None
        self.pickup_lat = None
        self.dropoff_lat = None
        self.dropoff_lon = None

    def to_csv(self):
        fields = [
            self.event_id,
            self.timestamp,
            self.order_status.value,
            self.city_name.value,
            self.order_id,
            self.user_id,
            self.restaurant_id,
            self.courier_id,
            self.order_value,
            self.delivery_fee,
            self.pickup_time,
            self.delivery_time,
            self.wait_time,
            self.pickup_lat,
            self.pickup_lon,
            self.dropoff_lat,
            self.dropoff_lon,
        ]
        return ','.join(map(str, fields))
    

# order_id -> current_state (event_types)
active_orders = {}
# order_id -> metadata (stad, user, restaurang, coords, tider osv)
order_metadata = {}
next_order_id = 1
next_event_id = 1

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
    "dropoff_lon",
]


def init_csv():
    """Skapar raw_events.csv med header"""
    with open(RAW_CSV_PATH, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADER)


def handle_event(event: Event):
    """
    Tar ett Event-objekt och skriver ut det som en rad i raw_events.csv.
    En rad per event, i samma ordning som CSV_HEADER.
    """
    row = [
        event.event_id,
        event.timestamp,
        event.order_status.value if event.order_status is not None else None,
        event.city_name.value if event.city_name is not None else None,
        event.order_id,
        event.user_id,
        event.restaurant_id,
        event.courier_id,
        event.order_value,
        event.delivery_fee,
        event.pickup_time,
        event.delivery_time,
        event.wait_time,
        event.pickup_lat,
        event.pickup_lon,
        event.dropoff_lat,
        event.dropoff_lon,
    ]

    with open(RAW_CSV_PATH, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)


def choose_city() -> city:
    """Välj en stad slumpmässigt."""
    return random.choice([city.STOCKHOLM, city.MALMO, city.LINKOPING])


def random_dropoff_for_city(city_name: city):
    """Slumpa en dropoff-position inom stadens bounding box."""
    if city_name == city.STOCKHOLM:
        lat_range, lon_range = stockholm_lat_range, stockholm_lon_range
    elif city_name == city.MALMO:
        lat_range, lon_range = malmo_lat_range, malmo_lon_range
    elif city_name == city.LINKOPING:
        lat_range, lon_range = linkoping_lat_range, linkoping_lon_range
    else:
        raise ValueError(f"Unknown city: {city_name}")

    lat = random.uniform(lat_range[0], lat_range[1])
    lon = random.uniform(lon_range[0], lon_range[1])
    return lat, lon


def pick_restaurant_for_city(city_name: city):
    """
    Välj en restaurang för given stad.
    Returnerar (restaurant_id, pickup_lat, pickup_lon).
    """
    if city_name == city.STOCKHOLM:
        lst = stockholm_resturaunt_cord_list
    elif city_name == city.MALMO:
        lst = malmo_resturaunt_cord_list
    elif city_name == city.LINKOPING:
        lst = linkoping_resturaunt_id_cord_list
    else:
        raise ValueError(f"Unknown city: {city_name}")

    restaurant_id, lat, lon = random.choice(lst)
    return restaurant_id, lat, lon


def advance_order_state(order_id: int):
    """
    Flyttar en order ett steg framåt i sin state machine baserat på
    ORDER_STATE_TRANSITIONS och viktade sannolikheter.

    Returnerar:
        next_state (event_types) om ordern avancerades
        None om ordern redan är i ett terminalt state
    """
    # Hämta nuvarande state för ordern
    current_state = active_orders.get(order_id)
    if current_state is None:
        # Okänd order_id - upp till dig hur du vill hantera detta
        return None

    # Hämta möjliga transitions från nuvarande state
    transitions = ORDER_STATE_TRANSITIONS.get(current_state, [])
    if not transitions:
        # Inga transitions definierade = terminalt state
        return None

    # Slumpa ett tal mellan 0.0 och 1.0
    r = random.random()
    cumulative = 0.0

    # Gå igenom alla (next_state, weight) och hitta vilket intervall r hamnar i
    for next_state, weight in transitions:
        cumulative += weight
        if r <= cumulative:
            # Vi har hittat nästa state
            active_orders[order_id] = next_state
            return next_state

    # Om vikterna inte summerar exakt till 1.0 kan vi falla tillbaka på sista state
    last_state = transitions[-1][0]
    active_orders[order_id] = last_state
    return last_state


def create_new_order():
    """Skapa en ny order + första ORDER_PLACED-event."""
    global next_order_id
    global next_event_id

    order_id = next_order_id
    event_id = next_event_id
    next_order_id += 1
    next_event_id += 1

    city_name = choose_city()
    user_id = random.choice(user_id_list)

    restaurant_id, pickup_lat, pickup_lon = pick_restaurant_for_city(city_name)
    dropoff_lat, dropoff_lon = random_dropoff_for_city(city_name)

    order_value = round(random.uniform(80, 350), 2)
    delivery_fee = round(random.uniform(20, 49), 2)
    created_ts = int(time.time())

    # lagra metadata
    order_metadata[order_id] = {
        "city_name": city_name,
        "user_id": user_id,
        "restaurant_id": restaurant_id,
        "courier_id": None,
        "order_value": order_value,
        "delivery_fee": delivery_fee,
        "pickup_lat": pickup_lat,
        "pickup_lon": pickup_lon,
        "dropoff_lat": dropoff_lat,
        "dropoff_lon": dropoff_lon,
        "created_time": created_ts,
        "pickup_time": -1,
        "delivery_time": -1,
    }

    active_orders[order_id] = event_types.ORDER_PLACED

    e = Event()
    e.event_id = event_id
    e.timestamp = created_ts
    e.order_status = event_types.ORDER_PLACED
    e.city_name = city_name

    e.order_id = order_id
    e.user_id = user_id
    e.restaurant_id = restaurant_id
    e.courier_id = -1  # ingen kurir än
    e.order_value = order_value
    e.delivery_fee = delivery_fee

    e.pickup_time = -1
    e.delivery_time = -1
    e.wait_time = -1

    e.pickup_lat = pickup_lat
    e.pickup_lon = pickup_lon
    e.dropoff_lat = dropoff_lat
    e.dropoff_lon = dropoff_lon

    handle_event(e)


def advance_existing_order(order_id: int):
    """Avancera en befintlig order ett steg och generera event."""
    meta = order_metadata.get(order_id)
    if meta is None:
        return

    next_state = advance_order_state(order_id)
    if next_state is None:
        return

    now_ts = int(time.time())

    # uppdatera metadata beroende på state
    if next_state == event_types.COURIER_ASSIGNED and meta["courier_id"] is None:
        meta["courier_id"] = random.choice(courier_id_list)

    if next_state == event_types.ORDER_PICKED_UP and meta["pickup_time"] == -1:
        meta["pickup_time"] = now_ts

    if next_state == event_types.ORDER_DELIVERED and meta["delivery_time"] == -1:
        meta["delivery_time"] = now_ts

    e = Event()
    global next_event_id
    event_id = next_event_id
    next_event_id += 1
    e.event_id = event_id
    e.timestamp = now_ts
    e.order_status = next_state
    e.city_name = meta["city_name"]

    e.order_id = order_id
    e.user_id = meta["user_id"]
    e.restaurant_id = meta["restaurant_id"]
    e.courier_id = meta["courier_id"] if meta["courier_id"] is not None else -1
    e.order_value = meta["order_value"]
    e.delivery_fee = meta["delivery_fee"]

    e.pickup_time = meta["pickup_time"]
    e.delivery_time = meta["delivery_time"]

    if meta["pickup_time"] != -1:
        e.wait_time = meta["pickup_time"] - meta["created_time"] if "created_time" in meta else -1
    else:
        e.wait_time = -1

    e.pickup_lat = meta["pickup_lat"]
    e.pickup_lon = meta["pickup_lon"]
    e.dropoff_lat = meta["dropoff_lat"]
    e.dropoff_lon = meta["dropoff_lon"]

    handle_event(e)

    # om state är terminalt → städa bort
    if next_state in (
        event_types.ORDER_DELIVERED,
        event_types.ORDER_CANCELLED_USR,
        event_types.ORDER_CANCELLED_REST,
        event_types.ORDER_CANCELLED_COUR,
    ):
        active_orders.pop(order_id, None)
        order_metadata.pop(order_id, None)







# --- Själva producer-loopen ---
def run_producer():
    init_csv()

    while True:
        r = random.random()

        if r < 0.4:
            # skapa ny order
            create_new_order()

        elif r < 1 and active_orders:
            # uppdatera befintlig order
            order_id = random.choice(list(active_orders.keys()))
            advance_existing_order(order_id)

        else:
            # här skulle du kunna lägga in t.ex. PAYMENT_FAILED / APPLICATION_ERROR / API_FAILED
            # just nu: gör ingenting
            pass

        time.sleep(1)