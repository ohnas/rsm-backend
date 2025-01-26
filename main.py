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

load_dotenv()

# DATE_TO = "2025-01-16"
# DATE_FROM = "2025-01-24"
DATE_SINCE = "2024-10-01"
DATE_UNTILL = "2024-10-31"

ttc_info = {
    "imweb_api_key": os.getenv("IMWEB_API_KEY_TTC"),
    "imweb_api_secret": os.getenv("IMWEB_API_SECRET_TTC"),
    "meta_act_id": os.getenv("META_ACT_ID_TTC"),
    "meta_app_accesstoken": os.getenv("META_APP_ACCESSTOKEN"),
    "order_version": "v2",
    "mode_shipping": 3322,
    "imweb_order_table": "imweb_order_ttc",
    "imweb_order_detail_table": "imweb_order_detail_ttc",
    "meta_table": "meta_ttc",
}

anddle_info = {
    "imweb_api_key": os.getenv("IMWEB_API_KEY_ANDDLE"),
    "imweb_api_secret": os.getenv("IMWEB_API_SECRET_ANDDLE"),
    "meta_act_id": os.getenv("META_ACT_ID_ANDDLE"),
    "meta_app_accesstoken": os.getenv("META_APP_ACCESSTOKEN"),
    "order_version": "v2",
    "mode_shipping": 3102,
    "imweb_order_table": "imweb_order_anddle",
    "imweb_order_detail_table": "imweb_order_detail_anddle",
    "meta_table": "meta_anddle",
}


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

    # access_token, order_list, order_no_list = get_order_list(
    #     DATE_TO, DATE_FROM, anddle_info, conn
    # )
    # order_detail_list = get_order_detail_list(
    #     DATE_TO, order_no_list, access_token, anddle_info, conn
    # )
    # insert_imweb_order_table(DATE_TO, anddle_info, order_list, conn)
    # insert_imweb_order_detail_table(DATE_TO, anddle_info, order_detail_list, conn)
    meta_list = get_meta(DATE_SINCE, DATE_UNTILL, anddle_info, conn)
    insert_meta_table(DATE_SINCE, anddle_info, meta_list, conn)
    # exchange_rate_data = get_krw_exchange_rate(conn, DATE)
    # insert_exchange_rate_table(conn, exchange_rate_data, DATE)

finally:
    conn.close()
