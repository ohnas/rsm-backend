from tools import log_error, insert_log


def insert_imweb_order_table(date, brand_info, order_list, conn):
    allowed_tables = ["imweb_order_undirty", "imweb_order_ttc", "imweb_order_anddle"]

    if brand_info["imweb_order_table"] not in allowed_tables:
        raise ValueError(f"Invalid table name: {brand_info['imweb_order_table']}")

    sql = f"""
        INSERT INTO {brand_info['imweb_order_table']} (
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
            mode_shipping
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
            %(mode_shipping)s
        )
    """
    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql, order_list)

        conn.commit()
        print(f"success : insert {brand_info['imweb_order_table']}")
        insert_log(
            conn,
            date,
            "SUCCESS",
            f"Order inserted for {len(order_list)}",
            "imweb",
            f"{brand_info['brand']}",
        )
    except Exception as e:
        print("fail")
        log_error(e)
        insert_log(conn, date, "FAIL", str(e), "imweb", f"{brand_info['brand']}")


def insert_imweb_order_detail_table(date, brand_info, order_detail_list, conn):
    allowed_tables = [
        "imweb_order_detail_undirty",
        "imweb_order_detail_ttc",
        "imweb_order_detail_anddle",
    ]

    if brand_info["imweb_order_detail_table"] not in allowed_tables:
        raise ValueError(
            f"Invalid table name: {brand_info['imweb_order_detail_table']}"
        )

    sql = f"""
        INSERT INTO {brand_info['imweb_order_detail_table']} (
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
        print(f"success : insert {brand_info['imweb_order_detail_table']}")
        insert_log(
            conn,
            date,
            "SUCCESS",
            f"Order inserted for {len(order_detail_list)}",
            "imweb",
            f"{brand_info['brand']}",
        )
    except Exception as e:
        print("fail")
        log_error(e)
        insert_log(conn, date, "FAIL", str(e), "imweb", f"{brand_info['brand']}")


def insert_meta_table(date, brand_info, meta_list, conn):
    allowed_tables = ["meta_undirty", "meta_ttc", "meta_anddle"]

    if brand_info["meta_table"] not in allowed_tables:
        raise ValueError(f"Invalid table name: {brand_info['meta_table']}")

    sql = f"""
        INSERT INTO {brand_info['meta_table']} (
            account_currency,
            account_id,
            account_name,
            campaign_id,
            campaign_name,
            adset_id,
            adset_name,
            ad_id,
            ad_name,
            objective,
            spend,
            link_click,
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
            `like`,
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
            %(account_currency)s,
            %(account_id)s,
            %(account_name)s,
            %(campaign_id)s,
            %(campaign_name)s,
            %(adset_id)s,
            %(adset_name)s,
            %(ad_id)s,
            %(ad_name)s,
            %(objective)s,
            %(spend)s,
            %(link_click)s,
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
        print(f"success : insert {brand_info['meta_table']}")
        insert_log(
            conn,
            date,
            "SUCCESS",
            f"meta inserted for {len(meta_list)}",
            "meta",
            f"{brand_info['brand']}",
        )
    except Exception as e:
        print("fail")
        log_error(e)
        insert_log(conn, date, "FAIL", str(e), "meta", f"{brand_info['brand']}")


def insert_exchange_rate_table(date, exchange_rate_data, conn):

    sql = """
        INSERT INTO exchange_rate (
            date,
            krw
        ) VALUES (
            %(date)s,
            %(krw)s
        )
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, exchange_rate_data)

        conn.commit()
        print("success : insert insert_exchange_rate_table")
        insert_log(
            conn,
            date,
            "SUCCESS",
            "-",
            "exchange_rate",
            "-",
        )
    except Exception as e:
        print("fail")
        log_error(e)
        insert_log(conn, date, "FAIL", str(e), "exchange_rate", "-")


def insert_smartstore_order_table(date, brand_info, order_list, conn):
    allowed_tables = ["smartstore_order_undirty"]

    if brand_info["smartstore_order_table"] not in allowed_tables:
        raise ValueError(f"Invalid table name: {brand_info['smartstore_order_table']}")

    sql = f"""
        INSERT INTO {brand_info['smartstore_order_table']} (
            product_order_id,
            order_id,
            order_date,
            payment_date,
            payment_means,
            pay_location_type,
            order_discount_amount,
            general_payment_amount,
            naver_mileage_payment_amount,
            charge_amount_payment_amount,
            pay_later_payment_amount,
            quantity,
            initial_quantity,
            remain_quantity,
            total_product_amount,
            initial_product_amount,
            remain_product_amount,
            total_payment_amount,
            initial_payment_amount,
            remain_payment_amount,
            product_order_status,
            product_id,
            product_name,
            unit_price,
            product_class,
            original_product_id,
            merchant_channel_id,
            item_no,
            product_option,
            option_code,
            option_price,
            mall_id,
            inflow_path,
            inflow_path_add,
            product_discount_amount,
            initial_product_discount_amount,
            remain_product_discount_amount,
            seller_burden_discount_amount,
            product_imediate_discount_amount,
            seller_burden_imediate_discount_amount,
            delivery_fee_amount,
            delivery_policy_type,
            section_delivery_fee,
            shipping_fee_type,
            delivery_discount_amount,
            commission_rating_type,
            commission_pre_pay_status,
            payment_commission,
            sale_commission,
            knowledge_shopping_selling_interlock_commission,
            channel_commission,
            expected_settlement_amount,
            mode_shipping
        ) VALUES (
            %(product_order_id)s,
            %(order_id)s,
            %(order_date)s,
            %(payment_date)s,
            %(payment_means)s,
            %(pay_location_type)s,
            %(order_discount_amount)s,
            %(general_payment_amount)s,
            %(naver_mileage_payment_amount)s,
            %(charge_amount_payment_amount)s,
            %(pay_later_payment_amount)s,
            %(quantity)s,
            %(initial_quantity)s,
            %(remain_quantity)s,
            %(total_product_amount)s,
            %(initial_product_amount)s,
            %(remain_product_amount)s,
            %(total_payment_amount)s,
            %(initial_payment_amount)s,
            %(remain_payment_amount)s,
            %(product_order_status)s,
            %(product_id)s,
            %(product_name)s,
            %(unit_price)s,
            %(product_class)s,
            %(original_product_id)s,
            %(merchant_channel_id)s,
            %(item_no)s,
            %(product_option)s,
            %(option_code)s,
            %(option_price)s,
            %(mall_id)s,
            %(inflow_path)s,
            %(inflow_path_add)s,
            %(product_discount_amount)s,
            %(initial_product_discount_amount)s,
            %(remain_product_discount_amount)s,
            %(seller_burden_discount_amount)s,
            %(product_imediate_discount_amount)s,
            %(seller_burden_imediate_discount_amount)s,
            %(delivery_fee_amount)s,
            %(delivery_policy_type)s,
            %(section_delivery_fee)s,
            %(shipping_fee_type)s,
            %(delivery_discount_amount)s,
            %(commission_rating_type)s,
            %(commission_pre_pay_status)s,
            %(payment_commission)s,
            %(sale_commission)s,
            %(knowledge_shopping_selling_interlock_commission)s,
            %(channel_commission)s,
            %(expected_settlement_amount)s,
            %(mode_shipping)s
        )
    """
    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql, order_list)

        conn.commit()
        print(f"success : insert {brand_info['smartstore_order_table']}")
        insert_log(
            conn,
            date,
            "SUCCESS",
            f"Order inserted for {len(order_list)}",
            "smartstore",
            f"{brand_info['brand']}",
        )
    except Exception as e:
        print("fail")
        log_error(e)
        insert_log(conn, date, "FAIL", str(e), "smartstore", f"{brand_info['brand']}")


def select_imweb_order_detail_table(date, brand_info, conn):
    allowed_tables = [
        "imweb_order_detail_undirty",
        "imweb_order_detail_ttc",
        "imweb_order_detail_anddle",
    ]

    if brand_info["imweb_order_detail_table"] not in allowed_tables:
        raise ValueError(
            f"Invalid table name: {brand_info['imweb_order_detail_table']}"
        )

    sql = f"""
        SELECT order_no 
        FROM {brand_info['imweb_order_detail_table']}
        WHERE status IN ('DELIVERING','STANDBY','PAY_COMPLETE','PAY_WAIT') 
        ORDER BY pay_time ASC 
        LIMIT 100
        """

    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()  # 여러 개의 행 가져오기
        print(
            f"success : select {brand_info['imweb_order_detail_table']} : {len(result)}"
        )
        insert_log(
            conn,
            date,
            "SUCCESS",
            f"selected for {len(result)}",
            "imweb",
            f"{brand_info['brand']}",
        )
        return result
    except Exception as e:
        print("fail")
        log_error(e)
        insert_log(conn, date, "FAIL", str(e), "imweb", f"{brand_info['brand']}")


def update_imweb_order_detail_table(date, brand_info, order_detail_change_list, conn):
    allowed_tables = [
        "imweb_order_detail_undirty",
        "imweb_order_detail_ttc",
        "imweb_order_detail_anddle",
    ]

    if brand_info["imweb_order_detail_table"] not in allowed_tables:
        raise ValueError(
            f"Invalid table name: {brand_info['imweb_order_detail_table']}"
        )
    if order_detail_change_list:
        sql = f"""
            UPDATE {brand_info['imweb_order_detail_table']} 
            SET 
                status = %(status)s,
                claim_status = %(claim_status)s,
                claim_type = %(claim_type)s,
                pay_time = FROM_UNIXTIME(%(pay_time)s),
                delivery_time = FROM_UNIXTIME(%(delivery_time)s),
                complete_time = FROM_UNIXTIME(%(complete_time)s)
            WHERE 
                order_detail_no = %(order_detail_no)s
        """

        try:
            with conn.cursor() as cursor:
                cursor.executemany(sql, order_detail_change_list)

            conn.commit()
            print(f"success : update {brand_info['imweb_order_detail_table']}")
            insert_log(
                conn,
                date,
                "SUCCESS",
                f"updated for {len(order_detail_change_list)}",
                "imweb",
                f"{brand_info['brand']}",
            )
        except Exception as e:
            print("fail")
            log_error(e)
            insert_log(conn, date, "FAIL", str(e), "imweb", f"{brand_info['brand']}")


def update_smartstore_order_table(date, brand_info, change_order_list, conn):
    allowed_tables = ["smartstore_order_undirty"]

    if brand_info["smartstore_order_table"] not in allowed_tables:
        raise ValueError(f"Invalid table name: {brand_info['smartstore_order_table']}")

    if change_order_list:
        sql = f"""
            UPDATE {brand_info['smartstore_order_table']} 
            SET 
                product_order_status = %(product_order_status)s
            WHERE 
                product_order_id = %(product_order_id)s
                AND
                order_id = %(order_id)s
        """

        try:
            with conn.cursor() as cursor:
                cursor.executemany(sql, change_order_list)

            conn.commit()
            print(f"success : update {brand_info['smartstore_order_table']}")
            insert_log(
                conn,
                date,
                "SUCCESS",
                f"updated for {len(change_order_list)}",
                "smartstore",
                f"{brand_info['brand']}",
            )

        except Exception as e:
            print("fail")
            log_error(e)
            insert_log(
                conn, date, "FAIL", str(e), "smartstore", f"{brand_info['brand']}"
            )
