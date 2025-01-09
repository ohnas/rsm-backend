from dotenv import load_dotenv
import os
import requests
import time
from tools import get_timestamps, log_error

load_dotenv()

API_KEY = os.getenv("IMWEB_API_KEY_TTC")
API_SECRET = os.getenv("IMWEB_API_SECRET_TTC")
ORDER_VERSION = "v2"


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
        order_no_list = set()
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
                "order_version": ORDER_VERSION,
            }
            response = requests.get(URL, headers=headers, params=params)
            json_data = response.json()
            results = json_data["data"]["list"]
            current_page = json_data["data"]["pagenation"]["current_page"]
            total_page = json_data["data"]["pagenation"]["total_page"]

            for result in results:
                order_no_list.add(result["order_no"])
                order_list.append(result)

            if results and total_page > 1:
                while current_page < total_page:
                    time.sleep(1.5)
                    current_page += 1
                    params["offset"] = current_page
                    response = requests.get(URL, headers=headers, params=params)
                    json_data = response.json()
                    new_results = json_data["data"]["list"]

                    for result in new_results:
                        if result["order_no"] not in order_no_list:
                            order_no_list.add(result["order_no"])
                            order_list.append(result)

        print("order list cnt:", len(order_list))
        print("order no list cnt:", len(order_no_list))
        print("success : order list from:", date_from)
        print("success : order list to:", date_to)
        return access_token, order_list, list(order_no_list)
    except Exception as e:
        log_error(e)


def get_order_detail_list(order_no_list, access_token):
    try:
        order_detail_list = []
        if order_no_list:
            for order_no in order_no_list:
                time.sleep(1)
                url = f"https://api.imweb.me/v2/shop/orders/{order_no}/prod-orders"
                headers = {
                    "Content-Type": "application/json",
                    "access-token": access_token,
                    "version": "latest",
                }
                params = {
                    "order_version": ORDER_VERSION,
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
                        "order_no": order_no,
                        "order_detail_no": result["order_no"],
                        "channel_order_item_no": result.get("channel_order_item_no"),
                        "status": result["status"],
                        "claim_status": result.get("claim_status"),
                        "claim_type": result.get("claim_type"),
                        "pay_time": (
                            None
                            if result.get("pay_time") in [0, None]
                            else result["pay_time"]
                        ),
                        "delivery_time": (
                            None
                            if result.get("delivery_time") in [0, None]
                            else result["delivery_time"]
                        ),
                        "complete_time": (
                            None
                            if result.get("complete_time") in [0, None]
                            else result["complete_time"]
                        ),
                        "prod_no": result["items"][0]["prod_no"],
                        "prod_name": result["items"][0]["prod_name"],
                        "prod_custom_code": result["items"][0]["prod_custom_code"],
                        "prod_sku_no": result["items"][0]["prod_sku_no"],
                        "prod_count": result["items"][0]["payment"].get("count", 0),
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
                        "prod_period_discount": result["items"][0]["payment"].get(
                            "period_discount", 0
                        ),
                        "prod_deliv_code": result["items"][0]["delivery"].get(
                            "deliv_code"
                        ),
                        "prod_deliv_price_mix": result["items"][0]["delivery"].get(
                            "deliv_price_mix"
                        ),
                        "prod_deliv_group_code": result["items"][0]["delivery"].get(
                            "deliv_group_code"
                        ),
                        "prod_deliv_type": result["items"][0]["delivery"].get(
                            "deliv_type"
                        ),
                        "prod_deliv_pay_type": result["items"][0]["delivery"].get(
                            "deliv_pay_type"
                        ),
                        "prod_deliv_price_type": result["items"][0]["delivery"].get(
                            "deliv_price_type"
                        ),
                        "option_is_mix": result["items"][0]["options"][0][0].get(
                            "is_mix"
                        ),
                        "option_detail_code": (
                            None
                            if result["items"][0].get("options") == None
                            else result["items"][0]["options"][0][0][
                                "option_detail_code"
                            ]
                        ),
                        "option_type": (
                            None
                            if result["items"][0].get("options") == None
                            else result["items"][0]["options"][0][0]["type"]
                        ),
                        "option_stock_sku_no": (
                            result["items"][0]["options"][0][0]["stock_sku_no"][0]
                            if result["items"][0].get("options")
                            and len(result["items"][0]["options"][0]) > 0
                            and len(
                                result["items"][0]["options"][0][0].get(
                                    "stock_sku_no", []
                                )
                            )
                            > 0
                            else None
                        ),
                        "option_code_list": (
                            None
                            if result["items"][0].get("options") == None
                            else result["items"][0]["options"][0][0][
                                "option_code_list"
                            ][0]
                        ),
                        "option_name_list": (
                            None
                            if result["items"][0].get("options") == None
                            else result["items"][0]["options"][0][0][
                                "option_name_list"
                            ][0]
                        ),
                        "value_code_list": (
                            None
                            if result["items"][0].get("options") == None
                            else result["items"][0]["options"][0][0]["value_code_list"][
                                0
                            ]
                        ),
                        "value_name_list": (
                            None
                            if result["items"][0].get("options") == None
                            else result["items"][0]["options"][0][0]["value_name_list"][
                                0
                            ]
                        ),
                        "option_count": (
                            result["items"][0]["payment"]["count"]
                            if result["items"][0].get("options") == None
                            else result["items"][0]["options"][0][0]["payment"]["count"]
                        ),
                        "option_price": (
                            result["items"][0]["payment"]["price"]
                            if result["items"][0].get("options") == None
                            else result["items"][0]["options"][0][0]["payment"]["price"]
                        ),
                        "option_deliv_price": (
                            result["items"][0]["payment"]["deliv_price"]
                            if result["items"][0].get("options") == None
                            else result["items"][0]["options"][0][0]["payment"][
                                "deliv_price"
                            ]
                        ),
                        "option_island_price": (
                            result["items"][0]["payment"]["island_price"]
                            if result["items"][0].get("options") == None
                            else result["items"][0]["options"][0][0]["payment"][
                                "island_price"
                            ]
                        ),
                    }
                    order_detail_list.append(dic)

        print("success : order detail list cnt : ", len(order_detail_list))
        return order_detail_list
    except Exception as e:
        log_error(e)
