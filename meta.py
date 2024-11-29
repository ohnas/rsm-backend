from dotenv import load_dotenv
import os
import json
import requests

load_dotenv()

META_ACT_ID_TTC = os.getenv("META_ACT_ID_TTC")
META_APP_ACCESSTOKEN = os.getenv("META_APP_ACCESSTOKEN")
ver = "v21.0"
insights = "account_currency,account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,date_start,clicks,conversions,cost_per_inline_link_click,cpm,cpp,ctr,frequency,impressions,purchase_roas,reach,spend,website_ctr,website_purchase_roas,actions,action_values"
url = f"https://graph.facebook.com/{ver}/act_{META_ACT_ID_TTC}/insights"
params = {
    "fields": insights,
    "access_token": META_APP_ACCESSTOKEN,
    "level": "ad",
    "breakdowns": "age,gender",
    "time_range": json.dumps(
        {
            "since": "2023-09-11",
            "until": "2023-09-11",
        }
    ),
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
for r in results:
    print(r)
