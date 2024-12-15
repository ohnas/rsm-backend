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


def insert_meta_accounts_table(conn, meta_list):

    sql = """
        INSERT IGNORE INTO meta_accounts (
            account_id,
            account_currency,
            account_name
        ) VALUES (
            %(account_id)s,
            %(account_currency)s,
            %(account_name)s
        )
    """
    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql, meta_list)

        conn.commit()
        print("success : insert meta_accounts_table")
    except Exception as e:
        print("fail")
        log_error(e)


def insert_meta_campaigns_table(conn, meta_list):

    sql = """
        INSERT IGNORE INTO meta_campaigns (
            account_id,
            campaign_id,
            campaign_name
        ) VALUES (
            %(account_id)s,
            %(campaign_id)s,
            %(campaign_name)s
        )
    """
    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql, meta_list)

        conn.commit()
        print("success : insert meta_campaigns_table")
    except Exception as e:
        print("fail")
        log_error(e)


def insert_meta_adsets_table(conn, meta_list):

    sql = """
        INSERT IGNORE INTO meta_adsets (
            campaign_id,
            adset_id,
            adset_name
        ) VALUES (
            %(campaign_id)s,
            %(adset_id)s,
            %(adset_name)s
        )
    """
    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql, meta_list)

        conn.commit()
        print("success : insert meta_adsets_table")
    except Exception as e:
        print("fail")
        log_error(e)


def insert_meta_ads_table(conn, meta_list):

    sql = """
        INSERT IGNORE INTO meta_ads (
            adset_id,
            ad_id,
            ad_name
        ) VALUES (
            %(adset_id)s,
            %(ad_id)s,
            %(ad_name)s
        )
    """
    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql, meta_list)

        conn.commit()
        print("success : insert meta_ads_table")
    except Exception as e:
        print("fail")
        log_error(e)


def insert_meta_table(conn, meta_list):

    sql = """
        INSERT INTO meta_ttc (
            ad_id,
            objective,
            spend,
            cost_per_inline_link_click,
            impressions,
            reach,
            frequency,
            cpm,
            cpp,
            ad_created_time,
            ad_updated_time,
            date_start,
            date_stop,
            like,
            comment,
            onsite_conversion_post_save,
            post_reaction,
            post,
            video_view,
            post_engagement,
            initiate_checkout,
            add_to_cart,
            purchase,
            add_payment_info,
            initiate_checkout_value,
            add_to_cart_value,
            purchase_value
        ) VALUES (
            %(type)s,
            %(ad_id)s,
            %(objective)s,
            %(spend)s,
            %(cost_per_inline_link_click)s,
            %(impressions)s,
            %(reach)s,
            %(frequency)s,
            %(cpm)s,
            %(cpp)s,
            %(ad_created_time)s,
            %(ad_updated_time)s,
            %(date_start)s,
            %(date_stop)s,
            %(like)s,
            %(comment)s,
            %(onsite_conversion_post_save)s,
            %(post_reaction)s,
            %(post)s,
            %(video_view)s,
            %(post_engagement)s,
            %(initiate_checkout)s,
            %(add_to_cart)s,
            %(purchase)s,
            %(add_payment_info)s,
            %(initiate_checkout_value)s,
            %(add_to_cart_value)s,
            %(purchase_value)s
        )
    """
    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql, meta_list)

        conn.commit()
        print("success : insert meta_ttc_table")
    except Exception as e:
        print("fail")
        log_error(e)
