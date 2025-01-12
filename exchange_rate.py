import requests
from tools import log_error


def get_krw_exchange_rate():
    try:
        date = "2025-01-11"
        URL = f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{date}/v1/currencies/usd.json"

        response = requests.get(URL)
        response.raise_for_status()
        data = response.json()

        # 데이터 가공 및 반환
        exchange_rate_data = {
            "date": data["date"],
            "krw": round(data["usd"]["krw"], 2),  # 소수점 2자리로 반올림
        }
        return exchange_rate_data
    except Exception as e:
        log_error(e)
