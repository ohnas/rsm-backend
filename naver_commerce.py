import time
import bcrypt
import pybase64
import requests
from tools import log_error, get_datetime_string, transfer_iso8601_timestamp, insert_log


def get_access_token(brand_info):
    try:
        timestamp = int(time.time() * 1000)
        password = brand_info["smartstore_api_id"] + "_" + str(timestamp)
        hashed = bcrypt.hashpw(
            password.encode("utf-8"),
            brand_info["smartstore_api_secret"].encode("utf-8"),
        )
        secret_sign = pybase64.standard_b64encode(hashed).decode("utf-8")
        URL = "https://api.commerce.naver.com/external/v1/oauth2/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "client_id": brand_info["smartstore_api_id"],
            "timestamp": timestamp,
            "grant_type": "client_credentials",
            "client_secret_sign": secret_sign,
            "type": "SELF",
        }
        response = requests.post(URL, headers=headers, data=data)
        json_data = response.json()
        access_token = json_data["access_token"]

        return access_token
    except Exception as e:
        log_error(e)


def get_order_list(date, brand_info, conn):
    access_token = get_access_token(brand_info)
    try:
        date_from, date_to = get_datetime_string(date)
        URL = (
            "https://api.commerce.naver.com/external/v1/pay-order/seller/product-orders"
        )
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {
            "from": date_from,
            "to": date_to,
            "rangeType": "ORDERED_DATETIME",
        }
        response = requests.get(URL, headers=headers, params=params)
        json_data = response.json()
        results = json_data["data"]["contents"]
        order_list = []

        if results:
            for result in results:
                dic = {
                    "product_order_id": result["productOrderId"],
                    "order_id": result["content"]["order"]["orderId"],
                    "order_date": transfer_iso8601_timestamp(
                        result["content"]["order"]["orderDate"]
                    ),
                    "payment_date": (
                        None
                        if result["content"]["order"].get("paymentDate") == None
                        else transfer_iso8601_timestamp(
                            result["content"]["order"]["paymentDate"]
                        )
                    ),
                    "payment_means": result["content"]["order"]["paymentMeans"],
                    "pay_location_type": result["content"]["order"]["payLocationType"],
                    "order_discount_amount": result["content"]["order"][
                        "orderDiscountAmount"
                    ],
                    "general_payment_amount": result["content"]["order"][
                        "generalPaymentAmount"
                    ],
                    "naver_mileage_payment_amount": result["content"]["order"][
                        "naverMileagePaymentAmount"
                    ],
                    "charge_amount_payment_amount": result["content"]["order"][
                        "chargeAmountPaymentAmount"
                    ],
                    "pay_later_payment_amount": result["content"]["order"][
                        "payLaterPaymentAmount"
                    ],
                    "quantity": result["content"]["productOrder"]["quantity"],
                    "initial_quantity": result["content"]["productOrder"][
                        "initialQuantity"
                    ],
                    "remain_quantity": result["content"]["productOrder"][
                        "remainQuantity"
                    ],
                    "total_product_amount": result["content"]["productOrder"][
                        "totalProductAmount"
                    ],
                    "initial_product_amount": result["content"]["productOrder"][
                        "initialProductAmount"
                    ],
                    "remain_product_amount": result["content"]["productOrder"][
                        "remainProductAmount"
                    ],
                    "total_payment_amount": result["content"]["productOrder"][
                        "totalPaymentAmount"
                    ],
                    "initial_payment_amount": result["content"]["productOrder"][
                        "initialPaymentAmount"
                    ],
                    "remain_payment_amount": result["content"]["productOrder"][
                        "remainPaymentAmount"
                    ],
                    "product_order_status": result["content"]["productOrder"][
                        "productOrderStatus"
                    ],
                    "product_id": result["content"]["productOrder"]["productId"],
                    "product_name": result["content"]["productOrder"]["productName"],
                    "unit_price": result["content"]["productOrder"]["unitPrice"],
                    "product_class": result["content"]["productOrder"]["productClass"],
                    "original_product_id": result["content"]["productOrder"][
                        "originalProductId"
                    ],
                    "merchant_channel_id": result["content"]["productOrder"][
                        "merchantChannelId"
                    ],
                    "item_no": result["content"]["productOrder"]["itemNo"],
                    "product_option": result["content"]["productOrder"].get(
                        "productOption"
                    ),
                    "option_code": result["content"]["productOrder"]["optionCode"],
                    "option_price": result["content"]["productOrder"]["optionPrice"],
                    "mall_id": result["content"]["productOrder"]["mallId"],
                    "inflow_path": result["content"]["productOrder"]["inflowPath"],
                    "inflow_path_add": result["content"]["productOrder"].get(
                        "inflowPathAdd"
                    ),
                    "product_discount_amount": result["content"]["productOrder"][
                        "productDiscountAmount"
                    ],
                    "initial_product_discount_amount": result["content"][
                        "productOrder"
                    ]["initialProductDiscountAmount"],
                    "remain_product_discount_amount": result["content"]["productOrder"][
                        "remainProductDiscountAmount"
                    ],
                    "seller_burden_discount_amount": result["content"]["productOrder"][
                        "sellerBurdenDiscountAmount"
                    ],
                    "product_imediate_discount_amount": result["content"][
                        "productOrder"
                    ].get("productImediateDiscountAmount"),
                    "seller_burden_imediate_discount_amount": result["content"][
                        "productOrder"
                    ].get("sellerBurdenImediateDiscountAmount"),
                    "delivery_fee_amount": result["content"]["productOrder"][
                        "deliveryFeeAmount"
                    ],
                    "delivery_policy_type": result["content"]["productOrder"][
                        "deliveryPolicyType"
                    ],
                    "section_delivery_fee": result["content"]["productOrder"][
                        "sectionDeliveryFee"
                    ],
                    "shipping_fee_type": result["content"]["productOrder"][
                        "shippingFeeType"
                    ],
                    "delivery_discount_amount": result["content"]["productOrder"][
                        "deliveryDiscountAmount"
                    ],
                    "commission_rating_type": result["content"]["productOrder"][
                        "commissionRatingType"
                    ],
                    "commission_pre_pay_status": result["content"]["productOrder"][
                        "commissionPrePayStatus"
                    ],
                    "payment_commission": result["content"]["productOrder"][
                        "paymentCommission"
                    ],
                    "sale_commission": result["content"]["productOrder"][
                        "saleCommission"
                    ],
                    "knowledge_shopping_selling_interlock_commission": result[
                        "content"
                    ]["productOrder"]["knowledgeShoppingSellingInterlockCommission"],
                    "channel_commission": result["content"]["productOrder"][
                        "channelCommission"
                    ],
                    "expected_settlement_amount": result["content"]["productOrder"][
                        "expectedSettlementAmount"
                    ],
                    "mode_shipping": brand_info["mode_shipping"],
                }
                order_list.append(dic)
        print("order list cnt : ", len(order_list))
        print("success : order list from : ", date_from)
        print("success : order list to : ", date_to)
        insert_log(
            conn,
            date,
            "SUCCESS",
            f"Order fetched for {len(order_list)}",
            "smartstore",
            f"{brand_info['brand']}",
        )
        return order_list
    except Exception as e:
        log_error(e)
        insert_log(conn, date, "FAIL", str(e), "smartstore", f"{brand_info['brand']}")
