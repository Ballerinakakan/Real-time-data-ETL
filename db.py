import sqlite3

DB_PATH = 'data/events.db'

def get_conn():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id INTEGER,
            timestamp INTEGER,
            order_status TEXT,
            city_name TEXT,
            order_id INTEGER,
            user_id INTEGER,
            restaurant_id INTEGER,
            courier_id INTEGER,
            order_value REAL,
            delivery_fee REAL,
            pickup_time INTEGER,
            delivery_time INTEGER,
            wait_time INTEGER,
            pickup_lat REAL,
            pickup_lon REAL,
            delivery_lat REAL,
            delivery_lon REAL
        );
    ''')
    conn.commit()
    conn.close()

def insert_event(ev):
    conn = get_conn()
    cursor = conn.cursor()
    sql = '''
        INSERT INTO events (
        event_id,
        timestamp,
        order_status,
        city_name,
        order_id,
        user_id,
        restaurant_id,
        courier_id,
        order_value,
        delivery_fee,
        pickup_time,
        delivery_time,
        wait_time,
        pickup_lat,
        pickup_lon,
        delivery_lat,
        delivery_lon
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
    '''
    values = (
        ev.event_id,
        ev.timestamp,
        ev.order_status,
        ev.city_name,
        ev.order_id,
        ev.user_id,
        ev.restaurant_id,
        ev.courier_id,
        ev.order_value,
        ev.delivery_fee,
        ev.pickup_time,
        ev.delivery_time,
        ev.wait_time,
        ev.pickup_lat,
        ev.pickup_lon,
        ev.dropoff_lat,
        ev.dropoff_lon
    )
    cursor.execute(sql, values)
    conn.commit()
    conn.close()

def drop_table():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS events;')
    conn.commit()
    conn.close()


def get_max_event_id():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT MAX(event_id) FROM events;')
    result = cursor.fetchone()
    conn.close()
    if result and result[0]:
        return result[0]
    return 0
