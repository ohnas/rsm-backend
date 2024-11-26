from imweb import get_order_list
from db import insert_imweb_order_table

DATE_FROM = "2023-09-14"
DATE_TO = "2023-09-14"

access_token, order_list, order_no_list = get_order_list("2023-09-14", "2023-09-14")
print(access_token)
print(order_list)
print(order_no_list)
