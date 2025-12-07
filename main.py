import threading
import producer
import etl_csv2sql
import db


if __name__ == "__main__":

    db.drop_table()
    db.init_db()


    producer_thread = threading.Thread(target=producer.run_producer)
    etl_thread = threading.Thread(target=etl_csv2sql.run_etl)

    producer_thread.start()
    etl_thread.start()

    producer_thread.join()
    etl_thread.join()