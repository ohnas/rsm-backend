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

DATE_SINCE = "2022-12-01"
DATE_UNTILL = "2022-12-31"
# DATE = "2025-01-29"

brand_info_list = [
    {
        "brand": "undirty",
        "imweb_api_key": os.getenv("IMWEB_API_KEY_UNDIRTY"),
        "imweb_api_secret": os.getenv("IMWEB_API_SECRET_UNDIRTY"),
        "meta_act_id": os.getenv("META_ACT_ID_UNDIRTY"),
        "meta_app_accesstoken": os.getenv("META_APP_ACCESSTOKEN"),
        "order_version": "v2",
        "mode_shipping": 3322,
        "imweb_order_table": "imweb_order_undirty",
        "imweb_order_detail_table": "imweb_order_detail_undirty",
        "meta_table": "meta_undirty",
    },
    # {
    #     "brand": "ttc",
    #     "imweb_api_key": os.getenv("IMWEB_API_KEY_TTC"),
    #     "imweb_api_secret": os.getenv("IMWEB_API_SECRET_TTC"),
    #     "meta_act_id": os.getenv("META_ACT_ID_TTC"),
    #     "meta_app_accesstoken": os.getenv("META_APP_ACCESSTOKEN"),
    #     "order_version": "v2",
    #     "mode_shipping": 3322,
    #     "imweb_order_table": "imweb_order_ttc",
    #     "imweb_order_detail_table": "imweb_order_detail_ttc",
    #     "meta_table": "meta_ttc",
    # },
    # {
    #     "brand": "anddle",
    #     "imweb_api_key": os.getenv("IMWEB_API_KEY_ANDDLE"),
    #     "imweb_api_secret": os.getenv("IMWEB_API_SECRET_ANDDLE"),
    #     "meta_act_id": os.getenv("META_ACT_ID_ANDDLE"),
    #     "meta_app_accesstoken": os.getenv("META_APP_ACCESSTOKEN"),
    #     "order_version": "v2",
    #     "mode_shipping": 3102,
    #     "imweb_order_table": "imweb_order_anddle",
    #     "imweb_order_detail_table": "imweb_order_detail_anddle",
    #     "meta_table": "meta_anddle",
    # },
]

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
    for brand_info in brand_info_list:
        # access_token, order_list, order_no_list = get_order_list(
        #     DATE, brand_info, conn
        # )
        # order_detail_list = get_order_detail_list(
        #     DATE, order_no_list, access_token, brand_info, conn
        # )
        # insert_imweb_order_table(DATE, brand_info, order_list, conn)
        # insert_imweb_order_detail_table(DATE, brand_info, order_detail_list, conn)
        meta_list = get_meta(DATE_SINCE, DATE_UNTILL, brand_info, conn)
        insert_meta_table(DATE_SINCE, brand_info, meta_list, conn)

    # exchange_rate_data = get_krw_exchange_rate(DATE, conn)
    # insert_exchange_rate_table(DATE, exchange_rate_data, conn)

finally:
    conn.close()
