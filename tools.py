from datetime import datetime, timezone, timedelta
import pytz
import inspect


def get_yesterday():
    kst = pytz.timezone("Asia/Seoul")
    now = datetime.now(pytz.utc).astimezone(kst)
    yesterday = now - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    return yesterday_str


def get_timestamps(date):
    local_timezone = timezone(timedelta(hours=9))
    date_from_str = f"{date} 00:00:00"
    date_to_str = f"{date} 23:59:59"
    # 문자열을 datetime 객체로 변환 (로컬 타임존 적용)
    local_time_from = datetime.strptime(date_from_str, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=local_timezone
    )
    local_time_to = datetime.strptime(date_to_str, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=local_timezone
    )
    timestamp_from = int(local_time_from.timestamp())
    timestamp_to = int(local_time_to.timestamp())

    return timestamp_from, timestamp_to


def temp_get_timestamps(date_from, date_to):
    local_timezone = timezone(timedelta(hours=9))
    date_from_str = f"{date_from} 00:00:00"
    date_to_str = f"{date_to} 23:59:59"
    # 문자열을 datetime 객체로 변환 (로컬 타임존 적용)
    local_time_from = datetime.strptime(date_from_str, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=local_timezone
    )
    local_time_to = datetime.strptime(date_to_str, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=local_timezone
    )
    timestamp_from = int(local_time_from.timestamp())
    timestamp_to = int(local_time_to.timestamp())

    return timestamp_from, timestamp_to


def get_datetime_string(date, timezone_offset="+09:00"):
    date_from = f"{date}T00:00:00.000{timezone_offset}"
    date_to = f"{date}T23:59:59.999{timezone_offset}"

    return date_from, date_to


def transfer_iso8601_timestamp(date):
    date = datetime.fromisoformat(date.replace("Z", "+00:00"))
    timestamp = date.strftime("%Y-%m-%d %H:%M:%S")

    return timestamp


def log_error(e):
    """현재 함수 이름과 오류 메시지를 동적으로 출력"""
    function_name = inspect.currentframe().f_back.f_code.co_name
    print(f"Error in {function_name}: {e}")


def insert_log(conn, date, status, message=None, platform=None, brand=None):

    function_name = inspect.currentframe().f_back.f_code.co_name

    sql = """
        INSERT INTO logs (date, function_name, status, message, platform, brand)
        VALUES (%(date)s, %(function_name)s, %(status)s, %(message)s, %(platform)s, %(brand)s)
    """
    with conn.cursor() as cursor:
        cursor.execute(
            sql,
            {
                "date": date,
                "function_name": function_name,
                "status": status,
                "message": message,
                "platform": platform,
                "brand": brand,
            },
        )
    conn.commit()
    print(f"Log recorded: {function_name} - {status}")
