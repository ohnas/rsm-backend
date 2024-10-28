from imweb import get_prod_list
import pymysql
from dotenv import load_dotenv
import os

# load_dotenv()

# DB_HOST = os.getenv("DB_HOST")
# DB_USER = os.getenv("DB_USER")
# DB_PASSWORD = os.getenv("DB_PASSWORD")
# DB_NAME = os.getenv("DB_NAME")

# conn = pymysql.connect(
#     host=DB_HOST,
#     user=DB_USER,
#     password=DB_PASSWORD,
#     database=DB_NAME,
#     charset="utf8mb4",
# )

# try:
#     with conn.cursor() as cursor:
#         sql = "SELECT * FROM date"
#         cursor.execute(sql)
#         results = cursor.fetchall()
#         print(results)

# finally:
#     conn.close()
