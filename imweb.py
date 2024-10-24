from dotenv import load_dotenv
import os
import requests
import time
from tools import get_timestamps

load_dotenv()

API_KEY = os.getenv("IMWEB_API_KEY_TTC")
API_SECRET = os.getenv("IMWEB_API_SECRET_TTC")


def get_access_token():
    URL = "https://api.imweb.me/v2/auth"
    params = {
        "key": API_KEY,
        "secret": API_SECRET,
    }
    response = requests.get(URL, params=params)
    json_data = response.json()
    access_token = json_data["access_token"]

    return access_token


def get_order_list():
    access_token = get_access_token()
    date_from = "2024-10-22"
    date_to = "2024-10-22"
    timestamp_from, timestamp_to = get_timestamps(date_from, date_to)

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
        "type": "npay",
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

    order_list = []
    if results:
        for result in results:
            dic = {
                "type": "npay",
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

    return order_list
