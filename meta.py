from dotenv import load_dotenv
import os
import json
import requests


load_dotenv()

META_ACT_ID_TTC = os.getenv("META_ACT_ID_TTC")
META_APP_ACCESSTOKEN = os.getenv("META_APP_ACCESSTOKEN")
ver = "v21.0"
insights = "account_currency,account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,objective,spend,cost_per_inline_link_click,impressions,reach,frequency,cpm,cpp,created_time,updated_time,actions,action_values"
url = f"https://graph.facebook.com/{ver}/act_{META_ACT_ID_TTC}/insights"
params = {
    "fields": insights,
    "access_token": META_APP_ACCESSTOKEN,
    "level": "ad",
    "time_range": json.dumps(
        {
            "since": "2023-09-11",
            "until": "2023-09-11",
        }
    ),
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


print(f"Total records fetched: {len(results)}")

meta_list = []
if results:
    for result in results:
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
            "objective": result["objective"],
            "spend": result["spend"],
            "cost_per_inline_link_click": result.get("cost_per_inline_link_click"),
            "impressions": result["impressions"],
            "reach": result["reach"],
            "frequency": result["frequency"],
            "cpm": result["cpm"],
            "cpp": result["cpp"],
            "ad_created_time": result["created_time"],
            "ad_updated_time": result["updated_time"],
            "date_start": result["date_start"],
            "date_stop": result["date_stop"],
            "like": next(
                (
                    action["value"]
                    for action in result.get("actions", [])
                    if action["action_type"] == "like"
                ),
                None,
            ),
            "comment": next(
                (
                    action["value"]
                    for action in result.get("actions", [])
                    if action["action_type"] == "comment"
                ),
                None,
            ),
            "onsite_conversion.post_save": next(
                (
                    action["value"]
                    for action in result.get("actions", [])
                    if action["action_type"] == "onsite_conversion.post_save"
                ),
                None,
            ),
            "post_reaction": next(
                (
                    action["value"]
                    for action in result.get("actions", [])
                    if action["action_type"] == "post_reaction"
                ),
                None,
            ),
            "post": next(
                (
                    action["value"]
                    for action in result.get("actions", [])
                    if action["action_type"] == "post"
                ),
                None,
            ),
            "video_view": next(
                (
                    action["value"]
                    for action in result.get("actions", [])
                    if action["action_type"] == "video_view"
                ),
                None,
            ),
            "post_engagement": next(
                (
                    action["value"]
                    for action in result.get("actions", [])
                    if action["action_type"] == "post_engagement"
                ),
                None,
            ),
            "initiate_checkout": next(
                (
                    action["value"]
                    for action in result.get("actions", [])
                    if action["action_type"] == "initiate_checkout"
                ),
                None,
            ),
            "add_to_cart": next(
                (
                    action["value"]
                    for action in result.get("actions", [])
                    if action["action_type"] == "add_to_cart"
                ),
                None,
            ),
            "purchase": next(
                (
                    action["value"]
                    for action in result.get("actions", [])
                    if action["action_type"] == "purchase"
                ),
                None,
            ),
            "add_payment_info": next(
                (
                    action["value"]
                    for action in result.get("actions", [])
                    if action["action_type"] == "add_payment_info"
                ),
                None,
            ),
            "initiate_checkout_value": next(
                (
                    action["value"]
                    for action in result.get("action_values", [])
                    if action["action_type"] == "initiate_checkout"
                ),
                None,
            ),
            "add_to_cart_value": next(
                (
                    action["value"]
                    for action in result.get("action_values", [])
                    if action["action_type"] == "add_to_cart"
                ),
                None,
            ),
            "purchase_value": next(
                (
                    action["value"]
                    for action in result.get("action_values", [])
                    if action["action_type"] == "purchase"
                ),
                None,
            ),
        }
        meta_list.append(dic)
print(meta_list)
