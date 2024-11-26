import pymysql
from dotenv import load_dotenv
import os
from imweb import get_order_list, get_order_detail_list
from db import insert_imweb_order_table, insert_imweb_order_detail_table

DATE_FROM = "2023-09-26"
DATE_TO = "2023-09-26"

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# CLOUD_DB_HOST = os.getenv("CLOUD_DB_HOST")
# CLOUD_DB_USER = os.getenv("CLOUD_DB_USER")
# CLOUD_DB_PASSWORD = os.getenv("CLOUD_DB_PASSWORD")
# CLOUD_DB_NAME = os.getenv("CLOUD_DB_NAME")

try:
    conn = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )

    # conn = pymysql.connect(
    #     host=CLOUD_DB_HOST,
    #     user=CLOUD_DB_USER,
    #     password=CLOUD_DB_PASSWORD,
    #     database=CLOUD_DB_NAME,
    #     charset="utf8mb4",
    #     cursorclass=pymysql.cursors.DictCursor,
    # )

    access_token, order_list, order_no_list = get_order_list(DATE_FROM, DATE_TO)
    order_detail_list = get_order_detail_list(order_no_list, access_token)
    insert_imweb_order_table(conn, order_list)
    insert_imweb_order_detail_table(conn, order_detail_list)

finally:
    conn.close()
