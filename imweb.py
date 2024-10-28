from dotenv import load_dotenv
import os
import requests
import time
from tools import get_timestamps, log_error

load_dotenv()

API_KEY = os.getenv("IMWEB_API_KEY_TTC")
API_SECRET = os.getenv("IMWEB_API_SECRET_TTC")


def get_access_token():
    try:
        URL = "https://api.imweb.me/v2/auth"
        params = {
            "key": API_KEY,
            "secret": API_SECRET,
        }
        response = requests.get(URL, params=params)
        json_data = response.json()
        access_token = json_data["access_token"]

        return access_token
    except Exception as e:
        log_error(e)


def get_order_list(date_from, date_to):
    access_token = get_access_token()
    try:
        timestamp_from, timestamp_to = get_timestamps(date_from, date_to)
        types = ["normal", "npay"]
        order_list = []
        for type in types:
            time.sleep(1.5)
            URL = "https://api.imweb.me/v2/shop/orders"
            headers = {
                "Content-Type": "application/json",
                "access-token": access_token,
                "version": "latest",
            }
            params = {
                "limit": 100,
                "order_date_from": timestamp_from,
                "order_date_to": timestamp_to,
                "type": type,
                "order_version": "v1",
            }
            response = requests.get(URL, headers=headers, params=params)
            json_data = response.json()
            current_page = json_data["data"]["pagenation"]["current_page"]
            total_page = json_data["data"]["pagenation"]["total_page"]
            results = json_data["data"]["list"]

            if results and total_page > 1:
                time.sleep(1.5)
                while current_page <= total_page:
                    time.sleep(1.5)
                    current_page += 1
                    params = {
                        "limit": 100,
                        "order_date_from": timestamp_from,
                        "order_date_to": timestamp_to,
                        "type": "npay",
                        "order_version": "v1",
                        "offset": current_page,
                    }
                    response = requests.get(URL, headers=headers, params=params)
                    json_data = response.json()
                    results.extend(json_data["data"]["list"])

            if results:
                for result in results:
                    dic = {
                        "type": type,
                        "order_no": result["order_no"],
                        "order_type": result["order_type"],
                        "is_gift": result["is_gift"],
                        "device": result["device"]["type"],
                        "order_time": result["order_time"],
                        "pay_type": result["payment"].get("pay_type"),
                        "pg_type": result["payment"].get("pg_type"),
                        "deliv_type": result["payment"].get("deliv_type"),
                        "deliv_pay_type": result["payment"].get("deliv_pay_type"),
                        "price_currency": result["payment"].get("price_currency"),
                        "total_price": result["payment"].get("total_price"),
                        "deliv_price": result["payment"].get("deliv_price"),
                        "payment_amount": result["payment"].get("payment_amount"),
                    }
                    order_list.append(dic)

        return order_list, access_token
    except Exception as e:
        log_error(e)


def get_prod_list():
    order_list, access_token = get_order_list("2024-10-27", "2024-10-27")
    try:
        prod_list = []
        if order_list:
            for order in order_list:
                time.sleep(0.5)
                url = f"https://api.imweb.me/v2/shop/orders/{order['order_no']}/prod-orders"
                headers = {
                    "Content-Type": "application/json",
                    "access-token": access_token,
                    "version": "latest",
                }
                params = {
                    "order_version": "v1",
                }
                # 코드가 200이 될 때까지 반복적으로 요청
                max_retries = 5
                retries = 0
                response = None
                while retries < max_retries:
                    response = requests.get(url, headers=headers, params=params)
                    json_data = response.json()
                    if json_data["code"] == 200:
                        break
                    else:
                        retries += 1
                        time.sleep(1)

                results = json_data["data"]
                for result in results:
                    dic = {
                        "prod_order_no": result["order_no"],
                        "prod_status": result["status"],
                        "prod_claim_status": result["claim_status"],
                        "prod_claim_type": result["claim_type"],
                        "pay_time": result["pay_time"],
                        "delivery_time": result["delivery_time"],
                        "complete_time": result["complete_time"],
                        "prod_no": result["items"][0]["prod_no"],
                        "prod_name": result["items"][0]["prod_name"],
                        "prod_custom_code": result["items"][0]["prod_custom_code"],
                        "prod_sku_no": result["items"][0]["prod_sku_no"],
                        "prod_price": result["items"][0]["payment"]["price"],
                        "prod_price_tax_free": result["items"][0]["payment"][
                            "price_tax_free"
                        ],
                        "prod_deliv_price_tax_free": result["items"][0]["payment"][
                            "deliv_price_tax_free"
                        ],
                        "prod_deliv_price": result["items"][0]["payment"][
                            "deliv_price"
                        ],
                        "prod_island_price": result["items"][0]["payment"][
                            "island_price"
                        ],
                        "prod_price_sale": result["items"][0]["payment"]["price_sale"],
                        "prod_point": result["items"][0]["payment"]["point"],
                        "prod_coupon": result["items"][0]["payment"]["coupon"],
                        "prod_membership_discount": result["items"][0]["payment"][
                            "membership_discount"
                        ],
                        "prod_period_discount": result["items"][0]["payment"][
                            "period_discount"
                        ],
                        "prod_deliv_code": result["items"][0]["delivery"]["deliv_code"],
                        "prod_deliv_price_mix": result["items"][0]["delivery"][
                            "deliv_price_mix"
                        ],
                        "prod_deliv_group_code": result["items"][0]["delivery"][
                            "deliv_group_code"
                        ],
                        "prod_deliv_type": result["items"][0]["delivery"]["deliv_type"],
                        "prod_deliv_pay_type": result["items"][0]["delivery"][
                            "deliv_pay_type"
                        ],
                        "prod_deliv_price_type": result["items"][0]["delivery"][
                            "deliv_price_type"
                        ],
                        "option_detail_code": result["items"][0]["options"][0][0][
                            "option_detail_code"
                        ],
                        "option_type": result["items"][0]["options"][0][0]["type"],
                        "option_stock_sku_no": result["items"][0]["options"][0][0][
                            "stock_sku_no"
                        ][0],
                        "option_code_list": result["items"][0]["options"][0][0][
                            "option_code_list"
                        ][0],
                        "option_name_list": result["items"][0]["options"][0][0][
                            "option_name_list"
                        ][0],
                        "value_code_list": result["items"][0]["options"][0][0][
                            "value_code_list"
                        ][0],
                        "value_name_list": result["items"][0]["options"][0][0][
                            "value_name_list"
                        ][0],
                        "option_count": result["items"][0]["options"][0][0]["payment"][
                            "count"
                        ],
                        "option_price": result["items"][0]["options"][0][0]["payment"][
                            "price"
                        ],
                        "option_deliv_price": result["items"][0]["options"][0][0][
                            "payment"
                        ]["deliv_price"],
                        "option_island_price": result["items"][0]["options"][0][0][
                            "payment"
                        ]["island_price"],
                    }
                    merged = {**order, **dic}
                    prod_list.append(merged)

        print(len(prod_list))
        return prod_list
    except Exception as e:
        log_error(e)
