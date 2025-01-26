import requests
from tools import log_error, insert_log


def get_krw_exchange_rate(date, conn):
    try:
        URL = f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{date}/v1/currencies/usd.json"

        response = requests.get(URL)
        response.raise_for_status()
        data = response.json()

        # 데이터 가공 및 반환
        exchange_rate_data = {
            "date": data["date"],
            "krw": round(data["usd"]["krw"], 2),  # 소수점 2자리로 반올림
        }
        insert_log(
            conn,
            date,
            "SUCCESS",
            "-",
            "exchange_rate",
            "-",
        )
        return exchange_rate_data
    except Exception as e:
        log_error(e)
        insert_log(conn, date, "FAIL", str(e), "exchange_rate", "-")
