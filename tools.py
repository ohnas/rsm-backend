from datetime import datetime, timezone, timedelta
import inspect


def get_timestamps(date_from, date_to):
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


def log_error(e):
    """현재 함수 이름과 오류 메시지를 동적으로 출력"""
    function_name = inspect.currentframe().f_back.f_code.co_name
    print(f"Error in {function_name}: {e}")
