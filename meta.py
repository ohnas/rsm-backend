from datetime import datetime
import json
import requests
from tools import log_error, insert_log


def to_int(value):
    return int(value) if value not in [None, ""] else None


def to_float(value):
    return float(value) if value not in [None, ""] else None


def to_date(value):
    return datetime.strptime(value, "%Y-%m-%d").date() if value else None


def get_action_value(actions, action_type):
    return next(
        (
            to_int(action["value"])
            for action in actions
            if action["action_type"] == action_type
        ),
        None,
    )


def get_action_value_from_values(action_values, action_type):
    return next(
        (
            to_float(action["value"])
            for action in action_values
            if action["action_type"] == action_type
        ),
        None,
    )


def get_meta(date_since, date_untill, brand_info, conn):
    try:
        ver = "v21.0"
        insights = "account_currency,account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,objective,spend,cost_per_inline_link_click,impressions,reach,frequency,cpm,cpp,created_time,updated_time,actions,action_values"
        url = (
            f"https://graph.facebook.com/{ver}/act_{brand_info['meta_act_id']}/insights"
        )
        params = {
            "fields": insights,
            "access_token": brand_info["meta_app_accesstoken"],
            "level": "ad",
            "time_range": json.dumps(
                {
                    "since": date_since,
                    "until": date_untill,
                }
            ),
            "time_increment": 1,
            "action_breakdowns": ["action_type"],
            "limit": 100,
        }

        results = []

        while url:
            response = requests.get(url, params=params if "?" not in url else None)
            if response.status_code == 200:
                json_data = response.json()
                results.extend(json_data.get("data", []))
                url = json_data.get("paging", {}).get("next")
            else:
                print(f"Error: {response.status_code}, {response.text}")
                break

        meta_list = []
        if results:
            for result in results:

                actions = result.get("actions", [])
                action_values = result.get("action_values", [])

                dic = {
                    "account_currency": result["account_currency"],
                    "account_id": result["account_id"],
                    "account_name": result["account_name"],
                    "campaign_id": result["campaign_id"],
                    "campaign_name": result["campaign_name"],
                    "adset_id": result["adset_id"],
                    "adset_name": result["adset_name"],
                    "ad_id": result["ad_id"],
                    "ad_name": result["ad_name"],
                    "objective": result.get("objective"),
                    "spend": to_float(result.get("spend")),
                    "cost_per_inline_link_click": to_float(
                        result.get("cost_per_inline_link_click")
                    ),
                    "impressions": to_int(result.get("impressions")),
                    "reach": to_int(result.get("reach")),
                    "frequency": to_float(result.get("frequency")),
                    "cpm": to_float(result.get("cpm")),
                    "cpp": to_float(result.get("cpp")),
                    "ad_created_time": to_date(result["created_time"]),
                    "ad_updated_time": to_date(result["updated_time"]),
                    "date_start": to_date(result["date_start"]),
                    "date_stop": to_date(result["date_stop"]),
                    "like": get_action_value(actions, "like"),
                    "comment": get_action_value(actions, "comment"),
                    "onsite_conversion_post_save": get_action_value(
                        actions, "onsite_conversion.post_save"
                    ),
                    "post_reaction": get_action_value(actions, "post_reaction"),
                    "post": get_action_value(actions, "post"),
                    "video_view": get_action_value(actions, "video_view"),
                    "post_engagement": get_action_value(actions, "post_engagement"),
                    "initiate_checkout": get_action_value(actions, "initiate_checkout"),
                    "add_to_cart": get_action_value(actions, "add_to_cart"),
                    "purchase": get_action_value(actions, "purchase"),
                    "add_payment_info": get_action_value(actions, "add_payment_info"),
                    "link_click": get_action_value(actions, "link_click"),
                    "initiate_checkout_value": get_action_value_from_values(
                        action_values, "initiate_checkout"
                    ),
                    "add_to_cart_value": get_action_value_from_values(
                        action_values, "add_to_cart"
                    ),
                    "purchase_value": get_action_value_from_values(
                        action_values, "purchase"
                    ),
                }
                meta_list.append(dic)
        print(f"meta total records fetched: {len(results)}")
        print(f"Total records meta data: {len(meta_list)}")
        print("meta list success from : ", date_since)
        print("meta list success to : ", date_untill)
        insert_log(
            conn,
            date_since,
            "SUCCESS",
            f"meta fetched for {len(meta_list)}",
            "meta",
            f"{brand_info['brand']}",
        )
        return meta_list

    except Exception as e:
        log_error(e)
        insert_log(conn, date_since, "FAIL", str(e), "meta", f"{brand_info['brand']}")
