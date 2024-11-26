import pymysql
from dotenv import load_dotenv
import os
from imweb import get_order_list

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

conn = pymysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME,
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)

# CLOUD_DB_HOST = os.getenv("CLOUD_DB_HOST")
# CLOUD_DB_USER = os.getenv("CLOUD_DB_USER")
# CLOUD_DB_PASSWORD = os.getenv("CLOUD_DB_PASSWORD")
# CLOUD_DB_NAME = os.getenv("CLOUD_DB_NAME")

# conn = pymysql.connect(
#     host=CLOUD_DB_HOST,
#     user=CLOUD_DB_USER,
#     password=CLOUD_DB_PASSWORD,
#     database=CLOUD_DB_NAME,
#     charset="utf8mb4",
#     cursorclass=pymysql.cursors.DictCursor,
# )


def insert_imweb_order_table():
    order_list = get_order_list("2023-09-14", "2023-09-14")
    sql = """
        INSERT INTO imweb_order_ttc (
            type,
            order_code,
            order_no,
            channel_order_no,
            order_time,
            order_type,
            is_gift,
            sale_channel_idx,
            device,
            complete_time,
            pay_type,
            pg_type,
            deliv_type,
            deliv_pay_type,
            price_currency,
            total_price,
            deliv_price,
            island_price,
            price_sale,
            point,
            coupon,
            membership_discount,
            period_discount,
            payment_amount,
            payment_time,
            avg_logis_expense
        ) VALUES (
            %(type)s,
            %(order_code)s,
            %(order_no)s,
            %(channel_order_no)s,
            FROM_UNIXTIME(%(order_time)s),
            %(order_type)s,
            %(is_gift)s,
            %(sale_channel_idx)s,
            %(device)s,
            FROM_UNIXTIME(%(complete_time)s),
            %(pay_type)s,
            %(pg_type)s,
            %(deliv_type)s,
            %(deliv_pay_type)s,
            %(price_currency)s,
            %(total_price)s,
            %(deliv_price)s,
            %(island_price)s,
            %(price_sale)s,
            %(point)s,
            %(coupon)s,
            %(membership_discount)s,
            %(period_discount)s,
            %(payment_amount)s,
            FROM_UNIXTIME(%(payment_time)s),
            %(avg_logis_expense)s
        )
    """
    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql, order_list)

        conn.commit()
        print("success")
    except Exception as e:
        print("fail")
        print(e)
    finally:
        conn.close()


insert_imweb_order_table()
