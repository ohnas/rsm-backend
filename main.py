import pymysql
from dotenv import load_dotenv
import os
from imweb import get_order_list, get_order_detail_list
from meta import get_meta
from exchange_rate import get_krw_exchange_rate
from db import (
    insert_imweb_order_table,
    insert_imweb_order_detail_table,
    insert_meta_table,
    insert_exchange_rate_table,
)

DATE_FROM = "2025-01-24"
DATE_TO = "2025-01-24"
DATE_SINCE = "2025-01-24"
DATE_UNTILL = "2025-01-24"
DATE = "2025-01-24"

load_dotenv()

CLOUD_DB_HOST = os.getenv("CLOUD_DB_HOST")
CLOUD_DB_USER = os.getenv("CLOUD_DB_USER")
CLOUD_DB_PASSWORD = os.getenv("CLOUD_DB_PASSWORD")
CLOUD_DB_NAME = os.getenv("CLOUD_DB_NAME")

try:
    conn = pymysql.connect(
        host=CLOUD_DB_HOST,
        user=CLOUD_DB_USER,
        password=CLOUD_DB_PASSWORD,
        database=CLOUD_DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )

    access_token, order_list, order_no_list = get_order_list(DATE_FROM, DATE_TO, conn)
    order_detail_list = get_order_detail_list(order_no_list, access_token, conn, DATE)
    insert_imweb_order_table(conn, order_list, DATE)
    insert_imweb_order_detail_table(conn, order_detail_list, DATE)
    meta_list = get_meta(conn, DATE_SINCE, DATE_UNTILL)
    insert_meta_table(conn, meta_list, DATE)
    exchange_rate_data = get_krw_exchange_rate(conn, DATE)
    insert_exchange_rate_table(conn, exchange_rate_data, DATE)

finally:
    conn.close()
