import pymysql
from dotenv import load_dotenv
import os
from openpyxl import load_workbook


load_dotenv()

CLOUD_DB_HOST = os.getenv("CLOUD_DB_HOST")
CLOUD_DB_USER = os.getenv("CLOUD_DB_USER")
CLOUD_DB_PASSWORD = os.getenv("CLOUD_DB_PASSWORD")
CLOUD_DB_NAME = os.getenv("CLOUD_DB_NAME")


conn = pymysql.connect(
    host=CLOUD_DB_HOST,
    user=CLOUD_DB_USER,
    password=CLOUD_DB_PASSWORD,
    database=CLOUD_DB_NAME,
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)

FILE_PATH = "/Users/ohnaseong/Downloads/2025_02_01.xlsx"
WORK_BOOK = load_workbook(FILE_PATH)


def insert_rocket_growth_order_table(conn):

    sheet = WORK_BOOK["rocket_growth_order_undirty"]
    records = list(sheet.iter_rows(values_only=True))[1:]
    data = []

    if records:
        for record in records:
            dic = {
                "date": record[0],
                "order_no": record[1],
                "transaction_type": record[2],
                "category_id": record[3],
                "category_name": record[4],
                "tax_type": record[5],
                "product_id": record[6],
                "option_id": record[7],
                "sku_id": record[8],
                "product_name": record[9],
                "option_name": record[10],
                "price": record[11],
                "quantity": record[12],
                "amount": record[13],
                "coupang_support_discount": record[14],
                "sales_amount": record[15],
                "instant_discount_coupon": record[16],
                "download_coupon": record[17],
                "seller_discount_coupon": record[18],
                "settlement_amount": record[19],
                "sales_commission_rate": record[20],
                "discounted_sales_commission_rate": record[21],
                "sales_commission": record[22],
                "sales_commission_vat": record[23],
            }
            data.append(dic)

        sql = """
            INSERT INTO rocket_growth_order_undirty (
                date,
                order_no,
                transaction_type,
                category_id,
                category_name,
                tax_type,
                product_id,
                option_id,
                sku_id,
                product_name,
                option_name,
                price,
                quantity,
                amount,
                coupang_support_discount,
                sales_amount,
                instant_discount_coupon,
                download_coupon,
                seller_discount_coupon,
                settlement_amount,
                sales_commission_rate,
                discounted_sales_commission_rate,
                sales_commission,
                sales_commission_vat
            ) VALUES (
                %(date)s,
                %(order_no)s,
                %(transaction_type)s,
                %(category_id)s,
                %(category_name)s,
                %(tax_type)s,
                %(product_id)s,
                %(option_id)s,
                %(sku_id)s,
                %(product_name)s,
                %(option_name)s,
                %(price)s,
                %(quantity)s,
                %(amount)s,
                %(coupang_support_discount)s,
                %(sales_amount)s,
                %(instant_discount_coupon)s,
                %(download_coupon)s,
                %(seller_discount_coupon)s,
                %(settlement_amount)s,
                %(sales_commission_rate)s,
                %(discounted_sales_commission_rate)s,
                %(sales_commission)s,
                %(sales_commission_vat)s
            )
        """
        try:
            with conn.cursor() as cursor:
                cursor.executemany(sql, data)
            conn.commit()
            print(
                f"Success: Inserted {len(data)} rows into rocket_growth_order_undirty"
            )
        except Exception as e:
            print(f"Error: {str(e)}")


def insert_rocket_growth_warehousing_table(conn):

    sheet = WORK_BOOK["rocket_growth_warehousing_undirty"]
    records = list(sheet.iter_rows(values_only=True))[1:]
    data = []

    if records:
        for record in records:
            dic = {
                "date": record[0],
                "transaction_type": record[1],
                "order_no": record[2],
                "shipping_no": record[3],
                "order_date": record[4],
                "product_id": record[5],
                "option_id": record[6],
                "sku_id": record[7],
                "product_name": record[8],
                "option_name": record[9],
                "price": record[10],
                "size": record[11],
                "center": record[12],
                "quantity": record[13],
                "purchase_quantity": record[14],
                "option_type": record[15],
                "fee": record[16],
                "discount": record[17],
                "net_amount": record[18],
            }
            data.append(dic)

        sql = """
            INSERT INTO rocket_growth_warehousing_undirty (
                date,
                transaction_type,
                order_no,
                shipping_no,
                order_date,
                product_id,
                option_id,
                sku_id,
                product_name,
                option_name,
                price,
                size,
                center,
                quantity,
                purchase_quantity,
                option_type,
                fee,
                discount,
                net_amount
            ) VALUES (
                %(date)s,
                %(transaction_type)s,
                %(order_no)s,
                %(shipping_no)s,
                %(order_date)s,
                %(product_id)s,
                %(option_id)s,
                %(sku_id)s,
                %(product_name)s,
                %(option_name)s,
                %(price)s,
                %(size)s,
                %(center)s,
                %(quantity)s,
                %(purchase_quantity)s,
                %(option_type)s,
                %(fee)s,
                %(discount)s,
                %(net_amount)s
            )
        """
        try:
            with conn.cursor() as cursor:
                cursor.executemany(sql, data)
            conn.commit()
            print(
                f"Success: Inserted {len(data)} rows into rocket_growth_warehousing_undirty"
            )
        except Exception as e:
            print(f"Error: {str(e)}")


def insert_rocket_growth_shipping_table(conn):

    sheet = WORK_BOOK["rocket_growth_shipping_undirty"]
    records = list(sheet.iter_rows(values_only=True))[1:]
    data = []

    if records:
        for record in records:
            dic = {
                "date": record[0],
                "transaction_type": record[1],
                "order_no": record[2],
                "shipping_no": record[3],
                "order_date": record[4],
                "product_id": record[5],
                "option_id": record[6],
                "sku_id": record[7],
                "product_name": record[8],
                "option_name": record[9],
                "price": record[10],
                "size": record[11],
                "total_size": record[12],
                "center": record[13],
                "quantity": record[14],
                "fee": record[15],
                "discount": record[16],
                "discounted_fee": record[17],
                "additional_fee": record[18],
                "net_amount": record[19],
            }
            data.append(dic)

        sql = """
            INSERT INTO rocket_growth_shipping_undirty (
                date,
                transaction_type,
                order_no,
                shipping_no,
                order_date,
                product_id,
                option_id,
                sku_id,
                product_name,
                option_name,
                price,
                size,
                total_size,
                center,
                quantity,
                fee,
                discount,
                discounted_fee,
                additional_fee,
                net_amount
            ) VALUES (
                %(date)s,
                %(transaction_type)s,
                %(order_no)s,
                %(shipping_no)s,
                %(order_date)s,
                %(product_id)s,
                %(option_id)s,
                %(sku_id)s,
                %(product_name)s,
                %(option_name)s,
                %(price)s,
                %(size)s,
                %(total_size)s,
                %(center)s,
                %(quantity)s,
                %(fee)s,
                %(discount)s,
                %(discounted_fee)s,
                %(additional_fee)s,
                %(net_amount)s
            )
        """
        try:
            with conn.cursor() as cursor:
                cursor.executemany(sql, data)
            conn.commit()
            print(
                f"Success: Inserted {len(data)} rows into rocket_growth_shipping_undirty"
            )
        except Exception as e:
            print(f"Error: {str(e)}")


# 모든 데이터베이스 연결 종료
conn.close()
