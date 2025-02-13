from dotenv import load_dotenv
import os
import time
import bcrypt
import pybase64
import requests
from tools import get_datetime_string

load_dotenv()

NAVER_COMMERCE_API_ID = os.getenv("NAVER_COMMERCE_API_ID")
NAVER_COMMERCE_API_SECRET = os.getenv("NAVER_COMMERCE_API_SECRET")
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

DATE_TO = "2025-02-12"
DATE_FROM = "2025-02-12"
date_to, date_from = get_datetime_string(DATE_T)
