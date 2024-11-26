from tools import log_error


def insert_imweb_order_table(conn, order_list):

    sql = """
        INSERT INTO imweb_order_ttc (
            type,
            order_code,
            order_no,
            channel_order_no,
            order_time,
            order_type,
            is_gift,
            sale_channel_idx,
            device,
            complete_time,
            pay_type,
            pg_type,
            deliv_type,
            deliv_pay_type,
            price_currency,
            total_price,
            deliv_price,
            island_price,
            price_sale,
            point,
            coupon,
            membership_discount,
            period_discount,
            payment_amount,
            payment_time,
            avg_logis_expense
        ) VALUES (
            %(type)s,
            %(order_code)s,
            %(order_no)s,
            %(channel_order_no)s,
            FROM_UNIXTIME(%(order_time)s),
            %(order_type)s,
            %(is_gift)s,
            %(sale_channel_idx)s,
            %(device)s,
            FROM_UNIXTIME(%(complete_time)s),
            %(pay_type)s,
            %(pg_type)s,
            %(deliv_type)s,
            %(deliv_pay_type)s,
            %(price_currency)s,
            %(total_price)s,
            %(deliv_price)s,
            %(island_price)s,
            %(price_sale)s,
            %(point)s,
            %(coupon)s,
            %(membership_discount)s,
            %(period_discount)s,
            %(payment_amount)s,
            FROM_UNIXTIME(%(payment_time)s),
            %(avg_logis_expense)s
        )
    """
    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql, order_list)

        conn.commit()
        print("success : insert imweb_order_table")
    except Exception as e:
        print("fail")
        log_error(e)


def insert_imweb_order_detail_table(conn, order_detail_list):

    sql = """
        INSERT INTO imweb_order_detail_ttc (
            order_no,
            order_detail_no,
            channel_order_item_no,
            status,
            claim_status,
            claim_type,
            pay_time,
            delivery_time,
            complete_time,
            prod_no,
            prod_name,
            prod_custom_code,
            prod_sku_no,
            prod_count,
            prod_price,
            prod_price_tax_free,
            prod_deliv_price_tax_free,
            prod_deliv_price,
            prod_island_price,
            prod_price_sale,
            prod_point,
            prod_coupon,
            prod_membership_discount,
            prod_period_discount,
            prod_deliv_code,
            prod_deliv_price_mix,
            prod_deliv_group_code,
            prod_deliv_type,
            prod_deliv_pay_type,
            prod_deliv_price_type,
            option_is_mix,
            option_detail_code,
            option_type,
            option_stock_sku_no,
            option_code_list,
            option_name_list,
            value_code_list,
            value_name_list,
            option_count,
            option_price,
            option_deliv_price,
            option_island_price
        ) VALUES (
            %(order_no)s,
            %(order_detail_no)s,
            %(channel_order_item_no)s,
            %(status)s,
            %(claim_status)s,
            %(claim_type)s,
            FROM_UNIXTIME(%(pay_time)s),
            FROM_UNIXTIME(%(delivery_time)s),
            FROM_UNIXTIME(%(complete_time)s),
            %(prod_no)s,
            %(prod_name)s,
            %(prod_custom_code)s,
            %(prod_sku_no)s,
            %(prod_count)s,
            %(prod_price)s,
            %(prod_price_tax_free)s,
            %(prod_deliv_price_tax_free)s,
            %(prod_deliv_price)s,
            %(prod_island_price)s,
            %(prod_price_sale)s,
            %(prod_point)s,
            %(prod_coupon)s,
            %(prod_membership_discount)s,
            %(prod_period_discount)s,
            %(prod_deliv_code)s,
            %(prod_deliv_price_mix)s,
            %(prod_deliv_group_code)s,
            %(prod_deliv_type)s,
            %(prod_deliv_pay_type)s,
            %(prod_deliv_price_type)s,
            %(option_is_mix)s,
            %(option_detail_code)s,
            %(option_type)s,
            %(option_stock_sku_no)s,
            %(option_code_list)s,
            %(option_name_list)s,
            %(value_code_list)s,
            %(value_name_list)s,
            %(option_count)s,
            %(option_price)s,
            %(option_deliv_price)s,
            %(option_island_price)s
        )
    """
    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql, order_detail_list)

        conn.commit()
        print("success : insert imweb_order_detail_table")
    except Exception as e:
        print("fail")
        log_error(e)
