import pymysql
from dotenv import load_dotenv
import os
from imweb import get_prod_list

prod_list = get_prod_list()

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
sql = """
    INSERT INTO imweb_ttc (
        type,
        order_no,
        order_type,
        is_gift,
        device,
        order_time,
        pay_type,
        pg_type,
        deliv_type,
        deliv_pay_type,
        price_currency,
        total_price,
        deliv_price,
        payment_amount,
        prod_order_no,
        prod_status,
        prod_claim_status,
        prod_claim_type,
        pay_time,
        delivery_time,
        complete_time,
        prod_no,
        prod_name,
        prod_custom_code,
        prod_sku_no,
        prod_price,
        prod_price_tax_free,
        prod_deliv_price_tax_free,
        prod_deliv_price,
        prod_island_price,
        prod_price_sale,
        prod_point,
        prod_coupon,
        prod_membership_discount,
        prod_period_discount,
        prod_deliv_code,
        prod_deliv_price_mix,
        prod_deliv_group_code,
        prod_deliv_type,
        prod_deliv_pay_type,
        prod_deliv_price_type,
        option_detail_code,
        option_type,
        option_stock_sku_no,
        option_code_list,
        option_name_list,
        value_code_list,
        value_name_list,
        option_count,
        option_price,
        option_deliv_price,
        option_island_price
    ) VALUES (
        %(type)s,
        %(order_no)s,
        %(order_type)s,
        %(is_gift)s,
        %(device)s,
        FROM_UNIXTIME(%(order_time)s),
        %(pay_type)s,
        %(pg_type)s,
        %(deliv_type)s,
        %(deliv_pay_type)s,
        %(price_currency)s,
        %(total_price)s,
        %(deliv_price)s,
        %(payment_amount)s,
        %(prod_order_no)s,
        %(prod_status)s,
        %(prod_claim_status)s,
        %(prod_claim_type)s,
        FROM_UNIXTIME(%(pay_time)s),
        FROM_UNIXTIME(%(delivery_time)s),
        FROM_UNIXTIME(%(complete_time)s),
        %(prod_no)s,
        %(prod_name)s,
        %(prod_custom_code)s,
        %(prod_sku_no)s,
        %(prod_price)s,
        %(prod_price_tax_free)s,
        %(prod_deliv_price_tax_free)s,
        %(prod_deliv_price)s,
        %(prod_island_price)s,
        %(prod_price_sale)s,
        %(prod_point)s,
        %(prod_coupon)s,
        %(prod_membership_discount)s,
        %(prod_period_discount)s,
        %(prod_deliv_code)s,
        %(prod_deliv_price_mix)s,
        %(prod_deliv_group_code)s,
        %(prod_deliv_type)s,
        %(prod_deliv_pay_type)s,
        %(prod_deliv_price_type)s,
        %(option_detail_code)s,
        %(option_type)s,
        %(option_stock_sku_no)s,
        %(option_code_list)s,
        %(option_name_list)s,
        %(value_code_list)s,
        %(value_name_list)s,
        %(option_count)s,
        %(option_price)s,
        %(option_deliv_price)s,
        %(option_island_price)s
    )
"""
try:
    with conn.cursor() as cursor:
        cursor.executemany(sql, prod_list)

    conn.commit()
    print("success")
except Exception as e:
    print("fail")
    print(e)
finally:
    conn.close()
