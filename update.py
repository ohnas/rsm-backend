import pymysql
from dotenv import load_dotenv
import os
from db import select_imweb_order_detail_table, update_imweb_order_detail_table
from imweb import update_order_detail_list

load_dotenv()

DATE = "2025-03-06"

brand_info_list = [
    {
        "brand": "ttc",
        "imweb_api_key": os.getenv("IMWEB_API_KEY_TTC"),
        "imweb_api_secret": os.getenv("IMWEB_API_SECRET_TTC"),
        "order_version": "v2",
        "imweb_order_detail_table": "imweb_order_detail_ttc",
    },
    {
        "brand": "anddle",
        "imweb_api_key": os.getenv("IMWEB_API_KEY_ANDDLE"),
        "imweb_api_secret": os.getenv("IMWEB_API_SECRET_ANDDLE"),
        "order_version": "v2",
        "imweb_order_detail_table": "imweb_order_detail_anddle",
    },
    {
        "brand": "undirty",
        "imweb_api_key": os.getenv("IMWEB_API_KEY_UNDIRTY"),
        "imweb_api_secret": os.getenv("IMWEB_API_SECRET_UNDIRTY"),
        "order_version": "v2",
        "imweb_order_detail_table": "imweb_order_detail_undirty",
    },
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
        order_no_list = select_imweb_order_detail_table(DATE, brand_info, conn)
        order_detail_change_list = update_order_detail_list(
            DATE, order_no_list, brand_info, conn
        )
        update_imweb_order_detail_table(
            DATE, brand_info, order_detail_change_list, conn
        )

finally:
    conn.close()
