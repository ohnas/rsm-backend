from dotenv import load_dotenv
import os
import time
import bcrypt
import pybase64
import requests
from tools import log_error, get_datetime_string

load_dotenv()

NAVER_COMMERCE_API_ID = os.getenv("NAVER_COMMERCE_API_ID")
NAVER_COMMERCE_API_SECRET = os.getenv("NAVER_COMMERCE_API_SECRET")
DATE_FROM = "2025-02-12"
DATE_TO = "2025-02-12"


def get_access_token():
    try:
        timestamp = int(time.time() * 1000)
        password = NAVER_COMMERCE_API_ID + "_" + str(timestamp)
        hashed = bcrypt.hashpw(
            password.encode("utf-8"), NAVER_COMMERCE_API_SECRET.encode("utf-8")
        )
        secret_sign = pybase64.standard_b64encode(hashed).decode("utf-8")
        URL = "https://api.commerce.naver.com/external/v1/oauth2/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "client_id": NAVER_COMMERCE_API_ID,
            "timestamp": timestamp,
            "grant_type": "client_credentials",
            "client_secret_sign": secret_sign,
            "type": "SELF",
        }
        response = requests.post(URL, headers=headers, data=data)
        json_data = response.json()
        access_token = json_data["access_token"]

        return access_token
    except Exception as e:
        log_error(e)


def get_order_detail_list():
    access_token = get_access_token()
    try:
        date_from, date_to = get_datetime_string(DATE_FROM, DATE_TO)
        URL = (
            "https://api.commerce.naver.com/external/v1/pay-order/seller/product-orders"
        )
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {
            "from": date_from,
            "to": date_to,
            "rangeType": "ORDERED_DATETIME",
        }
        response = requests.get(URL, headers=headers, params=params)
        json_data = response.json()
        results = json_data["data"]["contents"]
        print(len(results))
        for result in results:
            print(result)
            print(
                "==========================================================================================================="
            )
    except Exception as e:
        log_error(e)


get_order_detail_list()
