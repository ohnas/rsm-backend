from dotenv import load_dotenv
import os
import requests

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
