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

# DATE_FROM = "2025-01-11"
# DATE_TO = "2025-01-11"
DATE_SINCE = "2025-01-06"
DATE_UNTILL = "2025-01-11"
# DATE = "2025-01-11"

load_dotenv()

# DB_HOST = os.getenv("DB_HOST")
# DB_USER = os.getenv("DB_USER")
# DB_PASSWORD = os.getenv("DB_PASSWORD")
# DB_NAME = os.getenv("DB_NAME")

CLOUD_DB_HOST = os.getenv("CLOUD_DB_HOST")
CLOUD_DB_USER = os.getenv("CLOUD_DB_USER")
CLOUD_DB_PASSWORD = os.getenv("CLOUD_DB_PASSWORD")
CLOUD_DB_NAME = os.getenv("CLOUD_DB_NAME")

try:
    # conn = pymysql.connect(
    #     host=DB_HOST,
    #     user=DB_USER,
    #     password=DB_PASSWORD,
    #     database=DB_NAME,
    #     charset="utf8mb4",
    #     cursorclass=pymysql.cursors.DictCursor,
    # )

    conn = pymysql.connect(
        host=CLOUD_DB_HOST,
        user=CLOUD_DB_USER,
        password=CLOUD_DB_PASSWORD,
        database=CLOUD_DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )

    # access_token, order_list, order_no_list = get_order_list(DATE_FROM, DATE_TO, conn)
    # order_detail_list = get_order_detail_list(order_no_list, access_token, conn)
    # insert_imweb_order_table(conn, order_list)
    # insert_imweb_order_detail_table(conn, order_detail_list)
    meta_list = get_meta(DATE_SINCE, DATE_UNTILL)
    insert_meta_table(conn, meta_list)
    # exchange_rate_data = get_krw_exchange_rate(DATE)
    # insert_exchange_rate_table(conn, exchange_rate_data)

finally:
    conn.close()
