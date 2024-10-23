import requests
from imweb import get_access_token

access_token = get_access_token()
URL = "https://api.imweb.me/v2/shop/orders"
headers = {
    "Content-Type": "application/json",
    "access-token": access_token,
    "version": "latest",
}
params = {
    "limit": 100,
    "order_date_from": 1729522800,
    "order_date_to": 1729609199,
    "type": "npay",
    "order_version": "v1",
}
response = requests.get(URL, headers=headers, params=params)
json_data = response.json()
