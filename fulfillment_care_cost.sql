WITH adj AS (
    SELECT
        ar.order_uuid
        ,MAX_BY(reason, adjustment_timestamp_utc) adjustment_reason_name -- latest adjustment (on the off chance that there are multiple, won't duplicate order count when joining to order_fact/profit_fact)
        ,MAX_BY(COALESCE(sr.name, pr.name), adjustment_timestamp_utc) adj_contact_reason
    FROM
        source_cass_rainbow_data.adjustment_reporting ar
    LEFT JOIN
        integrated_core.ticket_fact tf
        ON tf.ticket_id = ar.ticket_id
        AND tf.ticket_created_Date BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_ADD('day', 1, DATE '{{end_date}}')
    LEFT JOIN
        source_zendesk_ref.primary_contact_reason pr
        ON pr.contact_reason_id = tf.primary_contact_reason
    LEFT JOIN
        source_zendesk_ref.secondary_contact_reason sr
        ON sr.contact_reason_id = tf.secondary_contact_reason
    WHERE
        DATE(DATE_PARSE(CAST(adjustment_dt AS VARCHAR), '%Y%m%d')) BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_ADD('day', 1, DATE '{{end_date}}')
        AND direction = 'ADJUST_DOWN' -- refund
        AND payer = 'GRUBHUB' -- paid by grubhub
    GROUP BY
        1
),
ghg AS (
    SELECT
        cart_uuid order_uuid
        ,MAX(
            CASE
                WHEN claim_type = 'SERVICE' THEN 'Late Delivery - GHG'
                WHEN claim_type = 'PRICING' THEN 'Price - GHG'
            END
        ) AS fg_reason
    FROM
        ods.carecontactfacade_guarantee_claim
    WHERE
        created_date BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_ADD('day', 1, DATE '{{end_date}}')
        AND decision = 'GRANT'
    GROUP BY
        1
),
care_fg AS (
    SELECT
        cr.order_uuid
        ,MAX_BY(COALESCE(sr.name, pr.name), issue_timestamp_utc) fg_reason
    FROM
        source_cass_rainbow_data.concession_reporting cr
    LEFT JOIN
        integrated_core.ticket_fact tf
        ON tf.ticket_id = cr.ticket_id
        AND tf.ticket_created_Date BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_ADD('day', 1, DATE '{{end_date}}')
    LEFT JOIN
        source_zendesk_ref.primary_contact_reason pr
        ON pr.contact_reason_id = tf.primary_contact_reason
    LEFT JOIN
        source_zendesk_ref.secondary_contact_reason sr
        ON sr.contact_reason_id = tf.secondary_contact_reason
    WHERE
        DATE(DATE_PARSE(CAST(expiration_dt AS VARCHAR), '%Y%m%d')) BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_ADD('day', 1, DATE '{{end_date}}')
    GROUP BY
        1
),
diner_ss_cancels AS (
    SELECT
        order_id order_uuid
        ,MAX(reason_code) reason_code
        ,MAX(
            CASE
                WHEN reason_code = 'DINER_PAYMENT_ISSUE' THEN 'Payment Issues'
                WHEN reason_code = 'DINER_CHOSE_WRONG_ADDRESS' THEN 'Delivery Information Incorrect'
                WHEN reason_code = 'DINER_CHOSE_WRONG_ORDER_ITEMS' THEN 'Ordered By Mistake'
                WHEN reason_code = 'DINER_DOES_NOT_WANT_LATE_ORDER' THEN 'Late Order'
                WHEN reason_code = 'DINER_DOES_NOT_WANT_THE_FOOD' THEN 'Change of Plans'
            END
        ) AS diner_ss_cancel_reason
        ,MAX(
            CASE
                WHEN reason_code = 'DINER_PAYMENT_ISSUE' THEN 'Diner Issues'
                WHEN reason_code = 'DINER_CHOSE_WRONG_ADDRESS' THEN 'Diner Issues'
                WHEN reason_code = 'DINER_CHOSE_WRONG_ORDER_ITEMS' THEN 'Diner Issues'
                WHEN reason_code = 'DINER_DOES_NOT_WANT_LATE_ORDER' THEN 'Logistics Issues'
                WHEN reason_code = 'DINER_DOES_NOT_WANT_THE_FOOD' THEN 'Diner Issues'
            END
        ) AS diner_ss_cancel_reason_group
    FROM
        ods.carereporting_cancellation_result
    WHERE
        created_date BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_ADD('day', 1, DATE '{{end_date}}')
    GROUP BY
        1
),
cancels AS (
    SELECT
        ocf.order_uuid
        ,order_status_cancel_ind
        ,CASE
            WHEN cancel_group = 'Not Mapped' AND diner_ss_cancel_reason_group IS NOT NULL THEN diner_ss_cancel_reason_group
            WHEN LOWER(order_status_cancel_reason) LIKE '%restaurant did%nt receive%' THEN 'Transmission Issues'
            ELSE cancel_group
        END cancel_group
        ,CASE
            WHEN cancel_reason_name = 'Not Mapped' AND diner_ss_cancel_reason IS NOT NULL THEN diner_ss_cancel_reason
            WHEN LOWER(order_status_cancel_reason) LIKE '%restaurant did%nt receive%' THEN 'restaurant did not receive order'
            ELSE cancel_reason_name
        END AS cancel_reason_name
        ,pr.name cancel_pcr
        ,sr.name cancel_scr
        ,COALESCE(cancellation_time_utc, tf.created_time) cancel_time_utc
        ,COALESCE(sr.name, pr.name) cancel_contact_reason
    FROM
        integrated_order.order_cancellation_fact ocf
    LEFT JOIN
        integrated_ref.cancellation_reason_map crm
        ON CAST(crm.cancel_reason_id AS VARCHAR) = CAST(ocf.primary_cancel_reason_id AS VARCHAR)
    LEFT JOIN
        integrated_core.ticket_fact tf
        ON tf.ticket_id = CAST(ocf.cancellation_ticket_id AS BIGINT)
        AND tf.ticket_created_Date BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_ADD('day', 1, DATE '{{end_date}}')
    LEFT JOIN
        source_zendesk_ref.primary_contact_reason pr
        ON pr.contact_reason_id = tf.primary_contact_reason
    LEFT JOIN
        source_zendesk_ref.secondary_contact_reason sr
        ON sr.contact_reason_id = tf.secondary_contact_reason
    LEFT JOIN
        diner_ss_cancels ccr
        ON ccr.order_uuid = ocf.order_uuid
    WHERE
        cancellation_date BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_ADD('day', 1, DATE '{{end_date}}')
),
mdf AS (
    SELECT
        mdf.order_uuid
        ,mdf.region_uuid
        ,mdf.region_name
        ,mealtime
        ,delivery_eta_type
        --- test regions vs control here
        ---add testing date cohorts based on date2 column
        ,CASE WHEN mdf.region_name LIKE 'CA%' THEN 'CA' ELSE 'xCA' END CA_Market
        ,CASE
            WHEN mdf.region_uuid IN (
                '92ecc187-d0ed-4b17-bcdb-7da84786f0ef',
                '71ba188f-e632-4e6c-8710-fa1b6b7a303e',
                'eea72701-225b-4f8c-a13d-b10798e7c89c',
                'd56fcd88-9b3f-47f9-a7a9-ac8cc8b15e89',
                '2c2aed8b-9f85-4666-873a-10f492b69dcb',
                'adad9d37-7a3b-46ab-9ca6-c0a354b107d6',
                'ac195f13-5f83-473e-b897-8c098c302699',
                'a213d9d3-78ac-4025-a9d3-6a00304f344c',
                'b2070f16-6d97-4c6e-9ce8-7ae581b4e87f',
                'f373b4d7-9e3e-4960-bd54-9122c51fc02f',
                '5d35d88c-fa24-488e-892c-b046ccfb9f61',
                '7bf6fde8-d453-4679-83bb-c67b737f12ed',
                '60fa2685-2463-48ba-b864-05384504ec2b',
                '504eb68a-9148-4b1f-bcff-941a7ba8f0e1',
                'a15298e0-4a9f-4f7d-aee5-507b7ed81651',
                'f138e5e9-d03e-4832-8f60-6eda4246c1aa',
                'afa226a3-f2e2-4d9b-8fa1-e228b8c7ed0d'
            ) THEN 'DCWP'
            ELSE 'xDCWP'
        END NYC_Market
        ,IF(mdf.bundle_type IS NOT NULL, TRUE, FALSE) bundle_ind
        ,IF(mdf.future_order_ind, TRUE, FALSE) AS future
        ,DATE(DATE_TRUNC('week', COALESCE(mdf.dropoff_complete_time_local, mdf.eta_at_order_placement_time_local, DATE_ADD('hour', 1, mdf.order_created_time_local)))) AS start_of_week
        ,DATE(COALESCE(mdf.dropoff_complete_time_local, mdf.eta_at_order_placement_time_local, DATE_ADD('hour', 1, mdf.order_created_time_local))) AS date2
        ,WEEK(COALESCE(mdf.dropoff_complete_time_local, mdf.eta_at_order_placement_time_local, DATE_ADD('hour', 1, mdf.order_created_time_local))) AS week
        ,MONTH(COALESCE(mdf.dropoff_complete_time_local, mdf.eta_at_order_placement_time_local, DATE_ADD('hour', 1, mdf.order_created_time_local))) AS month
        ,COALESCE(mdf.dropoff_complete_time_utc, mdf.eta_at_order_placement_time_utc, DATE_ADD('hour', 1, mdf.order_created_time_utc)) AS deliverytime_utc
        ,FORMAT_DATETIME(COALESCE(mdf.dropoff_complete_time_local, mdf.eta_at_order_placement_time_local, DATE_ADD('hour', 1, mdf.order_created_time_local)), 'E') AS dayofweek
        ,COALESCE(dropoff_complete_time_local, eta_at_order_placement_time_local, DATE_ADD('hour', 1, order_created_time_local)) AS datetime_local
        ,DATE_DIFF('second', IF(future_order_ind IS NULL OR future_order_ind = FALSE, mdf.order_created_time_utc, mdf.delivery_created_time_utc), lower_bound_eta_at_order_placement_time_utc) / 60.0 AS diner_ty_eta
        ,mdf.dropoff_complete_time_utc dropoff_complete_time_utc
        ,mdf.eta_at_order_placement_time_utc + INTERVAL '10' MINUTE ghd_eta_utc
        ,CASE WHEN DATE_DIFF('minute', mdf.eta_at_order_placement_time_utc + INTERVAL '10' MINUTE, mdf.dropoff_complete_time_utc) > 0 THEN 1 ELSE 0 END ghd_late_ind
        ,CASE WHEN cancelled_time_local IS NOT NULL THEN 1 ELSE 0 END AS cancel_ind
        ,CASE WHEN cancelled_time_local IS NOT NULL THEN DATE_DIFF('second', click_start_time_local, cancelled_time_local) / 60.0 ELSE NULL END AS cancel_mins
    FROM
        integrated_delivery.managed_delivery_fact_v2 mdf
    WHERE
        mdf.business_day BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_ADD('day', 1, DATE '{{end_date}}')
        AND COALESCE(DATE(mdf.dropoff_complete_time_local), DATE(mdf.eta_at_order_placement_time_local), DATE(DATE_ADD('hour', 1, mdf.order_created_time_local))) BETWEEN DATE '{{start_date}}' AND DATE '{{end_date}}'
),
contacts AS (
    SELECT
        order_uuid
        ,MAX_BY(ticket_id, created_time) AS latest_ticket_id
        ,MAX_BY(COALESCE(sr.name, pr.name), created_time) AS latest_contact_reason
        ,COUNT(ticket_id) contacts
    FROM
        integrated_core.ticket_fact tf
    LEFT JOIN
        source_zendesk_ref.primary_contact_reason pr
        ON pr.contact_reason_id = tf.primary_contact_reason
    LEFT JOIN
        source_zendesk_ref.secondary_contact_reason sr
        ON sr.contact_reason_id = tf.secondary_contact_reason
    WHERE
        ticket_created_date BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_ADD('day', 1, DATE '{{end_date}}')
        AND ticket_created_date < CURRENT_DATE
        AND order_uuid IS NOT NULL
        AND cpo_contact_indicator = 1 -- care worked contacts (automated contacts do not have a ticket cost)
    GROUP BY
        1
),
o AS (
    SELECT
        DATE(cpf.delivery_time_ct) date1
        ,region_uuid
        ,region_name
        ,CA_Market
        ,NYC_Market
        ,diner_ty_eta
        ,delivery_eta_type
        ,mealtime
        ,month
        ,start_of_week
        ,week
        ,date2
        ,deliverytime_utc
        ,dayofweek
        ,datetime_local
        ,CAST(CONCAT(CAST(HOUR(datetime_local) AS VARCHAR), ':', CAST(MINUTE(datetime_local) AS VARCHAR)) AS TIME) AS time_local
        ,IF(cpf.managed_delivery_ind = TRUE, 'ghd', 'non-ghd') ghd_ind
        ,cpf.order_uuid
        ,cp_diner_adj
        ,cancel_ind
        ,cancel_mins
        ,COALESCE(c.order_status_cancel_ind, FALSE) order_status_cancel_ind
        ,IF(c.order_uuid IS NOT NULL, TRUE, FALSE) cancel_fact_ind
        ,cancel_group
        ,cancel_reason_name
        ,cancel_contact_reason
        ,COALESCE(ghd_late_ind, 0) ghd_late_ind
        ,CASE
            WHEN ghd_late_ind = 1 THEN 1
            WHEN cancel_time_utc > ghd_eta_utc AND dropoff_complete_time_utc IS NULL THEN 1
            ELSE 0
        END ghd_late_ind_incl_cancel_time -- cancelled, no dropoff complete time
        ,CASE
            WHEN REGEXP_LIKE(LOWER(adjustment_reason_name), 'food temp|cold|quality_temp|temperature') THEN 'food temperature'
            WHEN REGEXP_LIKE(LOWER(adjustment_reason_name), 'incorrect order|incorrect item|wrong order|incorrect_item') THEN 'incorrect order'
            WHEN REGEXP_LIKE(LOWER(adjustment_reason_name), 'damaged') THEN 'food damaged'
            WHEN REGEXP_LIKE(LOWER(adjustment_reason_name), 'missing') THEN 'missing item'
            WHEN REGEXP_LIKE(LOWER(adjustment_reason_name), 'item removed') THEN 'item removed from order'
            WHEN REGEXP_LIKE(LOWER(adjustment_reason_name), 'late') THEN 'late order'
            WHEN REGEXP_LIKE(LOWER(adjustment_reason_name), 'menu error') THEN 'order or menu issue'
            WHEN REGEXP_LIKE(LOWER(adjustment_reason_name), 'temporarily unavailable|unavailable') THEN 'out of item'
            WHEN REGEXP_LIKE(LOWER(adjustment_reason_name), 'order not rec|missed delivery') THEN 'missed delivery'
            WHEN REGEXP_LIKE(LOWER(COALESCE(cancel_contact_reason, adj_contact_reason)), 'food temp|cold|quality_temp|temperature') THEN 'food temperature'
            WHEN REGEXP_LIKE(LOWER(COALESCE(cancel_contact_reason, adj_contact_reason)), 'incorrect order|incorrect item|wrong order|incorrect_item') THEN 'incorrect order'
            WHEN REGEXP_LIKE(LOWER(COALESCE(cancel_contact_reason, adj_contact_reason)), 'damaged') THEN 'food damaged'
            WHEN REGEXP_LIKE(LOWER(COALESCE(cancel_contact_reason, adj_contact_reason)), 'missing') THEN 'missing item'
            WHEN REGEXP_LIKE(LOWER(COALESCE(cancel_contact_reason, adj_contact_reason)), 'item removed') THEN 'item removed from order'
            WHEN REGEXP_LIKE(LOWER(COALESCE(cancel_contact_reason, adj_contact_reason)), 'late') THEN 'late order'
            WHEN REGEXP_LIKE(LOWER(COALESCE(cancel_contact_reason, adj_contact_reason)), 'menu error') THEN 'order or menu issue'
            WHEN REGEXP_LIKE(LOWER(COALESCE(cancel_contact_reason, adj_contact_reason)), 'temporarily unavailable|unavailable') THEN 'out of item'
            WHEN REGEXP_LIKE(LOWER(COALESCE(cancel_contact_reason, adj_contact_reason)), 'order not rec|missed delivery') THEN 'missed delivery'
            WHEN REGEXP_LIKE(LOWER(adjustment_reason_name), 'refund due to|refund for') THEN LOWER(COALESCE(cancel_contact_reason, adj_contact_reason, adjustment_reason_name))
            WHEN REGEXP_LIKE(LOWER(COALESCE(cancel_contact_reason, adj_contact_reason)), 'refund due to|refund for') THEN LOWER(COALESCE(cancel_contact_reason, adj_contact_reason, adjustment_reason_name))
            ELSE LOWER(COALESCE(cancel_contact_reason, adj_contact_reason, adjustment_reason_name)) -- use contact reason when there's a long reason (refund due to item 1 item 2 etc)
        END adjustment_reason_name
        ,adj_contact_reason
        ,driver_pay_per_order
        ,tip
        ,bundle_ind
        ,CASE
            WHEN cp_care_concession_awarded_amount = 0 THEN NULL -- no fg in profit fact
            WHEN REGEXP_LIKE(LOWER(COALESCE(ghg.fg_reason, care_fg.fg_reason)), 'food temp|cold|quality_temp|temperature') THEN 'food temperature'
            WHEN REGEXP_LIKE(LOWER(COALESCE(ghg.fg_reason, care_fg.fg_reason)), 'incorrect order|incorrect item|wrong order|incorrect_item') THEN 'incorrect order'
            WHEN REGEXP_LIKE(LOWER(COALESCE(ghg.fg_reason, care_fg.fg_reason)), 'damaged') THEN 'food damaged'
            WHEN REGEXP_LIKE(LOWER(COALESCE(ghg.fg_reason, care_fg.fg_reason)), 'missing') THEN 'missing item'
            WHEN REGEXP_LIKE(LOWER(COALESCE(ghg.fg_reason, care_fg.fg_reason)), 'item removed') THEN 'item removed from order'
            WHEN REGEXP_LIKE(LOWER(COALESCE(ghg.fg_reason, care_fg.fg_reason)), 'late') THEN 'late order'
            WHEN REGEXP_LIKE(LOWER(COALESCE(ghg.fg_reason, care_fg.fg_reason)), 'menu error') THEN 'order or menu issue'
            WHEN REGEXP_LIKE(LOWER(COALESCE(ghg.fg_reason, care_fg.fg_reason)), 'temporarily unavailable|unavailable') THEN 'out of item'
            WHEN REGEXP_LIKE(LOWER(COALESCE(ghg.fg_reason, care_fg.fg_reason)), 'order not rec|missed delivery') THEN 'missed delivery'
            -- WHEN REGEXP_LIKE(LOWER(adjustment_reason_name),'refund due to|refund for') THEN LOWER(COALESCE(cancel_contact_reason,adj_contact_reason,adjustment_reason_name))
            -- WHEN REGEXP_LIKE(LOWER(COALESCE(cancel_contact_reason,adj_contact_reason)),'refund due to|refund for') THEN LOWER(COALESCE(cancel_contact_reason,adj_contact_reason,adjustment_reason_name))
            ELSE LOWER(COALESCE(ghg.fg_reason, care_fg.fg_reason))
        END AS fg_reason
        ,ccr.care_cost_reason -- excludes contact reasons where we typically give refunds/freegrub, as reasons will be captured by fg reason and adj reason logic. If we change the logic, use contacts.latest_contact_reason
        ,COALESCE(ccr.care_cost_group, 'not grouped') care_cost_group -- excludes contact reasons where we typically give refunds/freegrub, as reasons will be captured by fg reason and adj reason logic
        ,cp_care_concession_awarded_amount
        ,cp_grub_care_refund
        ,cp_redelivery_cost
        ,cp_diner_care_tickets + cp_driver_care_tickets + cp_restaurant_care_tickets + cp_gh_internal_care_tickets cp_care_ticket_cost
    FROM
        integrated_order.order_contribution_profit_fact cpf
    -- LEFT JOIN integrated_order.order_cancellation_fact ocf ON ocf.order_uuid = cpf.order_uuid AND ocf.cancellation_date BETWEEN DATE_ADD('day',-1,DATE '{{start_date}}') AND DATE_ADD('day',1,DATE '{{end_date}}')
    JOIN
        mdf
        ON mdf.order_uuid = cpf.order_uuid
    LEFT JOIN
        cancels c
        ON c.order_uuid = cpf.order_uuid
    LEFT JOIN
        adj
        ON adj.order_uuid = cpf.order_uuid
    LEFT JOIN
        ghg
        ON ghg.order_uuid = cpf.order_uuid
    LEFT JOIN
        care_fg
        ON care_fg.order_uuid = cpf.order_uuid
    LEFT JOIN
        contacts
        ON contacts.order_uuid = cpf.order_uuid
    LEFT JOIN
        csv_sandbox.care_cost_reasons ccr
        ON ccr.scr = contacts.latest_contact_reason
    WHERE
        cpf.order_date BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_ADD('day', 1, DATE '{{end_date}}')
        AND managed_delivery_ind = TRUE
        --AND DATE(cpf.delivery_time_utc) BETWEEN DATE_ADD('day',-1,DATE '{{start_date}}') AND DATE_ADD('day',1,DATE '{{end_date}}')
        -- AND cp_diner_adj < 0 -- diner adjustment issued
),
o2 AS (
    SELECT
        -- ,eta_min_groupings
        date1
        ,region_uuid
        ,region_name
        ,CA_Market
        ,NYC_Market
        ,diner_ty_eta
        ,delivery_eta_type
        ,mealtime
        ,month
        ,start_of_week
        ,week
        ,date2
        ,deliverytime_utc
        ,dayofweek
        ,datetime_local
        ,time_local
        ,ghd_ind
        ,cancel_ind
        ,cancel_mins
        ,order_uuid
        ,cp_diner_adj
        ,cp_redelivery_cost
        ,order_status_cancel_ind
        ,cancel_fact_ind
        ,cancel_group
        ,cancel_reason_name
        ,cancel_contact_reason
        ,ghd_late_ind
        ,ghd_late_ind_incl_cancel_time
        ,CASE
            WHEN cancel_reason_name = 'Not Mapped' THEN adjustment_reason_name
            WHEN cancel_reason_name IS NULL THEN adjustment_reason_name
            ELSE LOWER(cancel_reason_name)
        END adjustment_and_cancel_reason_combined
        ,fg_reason
        ,cp_care_concession_awarded_amount
        ,driver_pay_per_order
        ,tip
        ,bundle_ind
        ,cp_care_ticket_cost
        ,cp_grub_care_refund
        ,care_cost_reason
        ,care_cost_group
    FROM
        o
),
o3 AS (
    SELECT
        date1
        -- ,eta_min_groupings
        ,region_uuid
        ,region_name
        ,CA_Market
        ,NYC_Market
        ,diner_ty_eta
        ,delivery_eta_type
        ,month
        ,mealtime
        ,start_of_week
        ,week
        ,date2
        ,deliverytime_utc
        ,dayofweek
        ,datetime_local
        ,time_local
        ,ghd_ind
        ,order_uuid
        ,cp_diner_adj
        ,cancel_ind
        ,cancel_mins
        ,cp_care_concession_awarded_amount + cp_care_ticket_cost + cp_diner_adj + IF(cp_redelivery_cost IS NULL, 0, cp_redelivery_cost) + IF(cp_grub_care_refund IS NULL, 0.00, cp_grub_care_refund) total_care_cost
        ,IF(cp_redelivery_cost IS NULL, 0, cp_redelivery_cost) AS cp_redelivery_cost
        ,order_status_cancel_ind
        ,cancel_fact_ind
        ,o2.cancel_group
        ,o2.cancel_reason_name
        ,cancel_contact_reason
        ,ghd_late_ind
        ,ghd_late_ind_incl_cancel_time
        ,CASE
            WHEN o2.cancel_group IS NOT NULL AND o2.cancel_group != 'Other' THEN crm.cancel_group
            WHEN REGEXP_LIKE(LOWER(adjustment_and_cancel_reason_combined), 'missed delivery|order not rec|late|damaged|ghd driver|food temp|quality_temp|cold') THEN 'Logistics Issues'
            WHEN REGEXP_LIKE(LOWER(adjustment_and_cancel_reason_combined), 'missing item|missing|incorrect_order|menu error|incorrect_item|missing_item|incorrect item|incorrect order|quality|special|problem with food|food poison|object in food|temporarily unavailable|out of item|item removed|foreign object') THEN 'Restaurant Issues'
            WHEN REGEXP_LIKE(LOWER(adjustment_and_cancel_reason_combined), 'diner error|switch to delivery or pickup|change of plans') THEN 'Diner Issues'
            ELSE COALESCE(crm.cancel_group, 'not grouped')
        END adjustment_group
        ,CASE
            WHEN o2.cancel_group IS NOT NULL AND o2.cancel_group != 'Other' THEN crm.cancel_group
            WHEN REGEXP_LIKE(LOWER(fg_reason), 'missed delivery|order not rec|late|damaged|ghd driver|food temp|quality_temp|cold') THEN 'Logistics Issues'
            WHEN REGEXP_LIKE(LOWER(fg_reason), 'missing item|missing|incorrect_order|menu error|incorrect_item|missing_item|incorrect item|incorrect order|quality|special|problem with food|food poison|object in food|temporarily unavailable|out of item|item removed|foreign object') THEN 'Restaurant Issues'
            WHEN REGEXP_LIKE(LOWER(fg_reason), 'diner error|switch to delivery or pickup|change of plans') THEN 'Diner Issues'
            ELSE COALESCE(crm.cancel_group, 'not grouped')
        END fg_group
        ,CASE
            WHEN cp_care_concession_awarded_amount < 0 AND fg_reason IS NULL THEN adjustment_and_cancel_reason_combined
            ELSE fg_reason
        END AS fg_reason
        ,cp_care_concession_awarded_amount
        ,adjustment_and_cancel_reason_combined
        ,driver_pay_per_order
        ,tip
        ,bundle_ind
        ,cp_care_ticket_cost
        ,care_cost_reason
        ,care_cost_group
    FROM
        o2
    LEFT JOIN
        integrated_ref.cancellation_reason_map crm
        ON LOWER(crm.cancel_reason_name) = LOWER(o2.adjustment_and_cancel_reason_combined)
)
SELECT
    *
FROM
    (
        SELECT
            CASE
                WHEN CA_Market = 'CA' THEN CA_Market
                WHEN NYC_Market = 'DCWP' THEN NYC_Market
                ELSE 'ROM'
            END AS cany_ind
            ,LOWER(
                CASE
                    WHEN total_care_cost = 0 THEN 'orders with no care cost' -- orders with no care cost
                    WHEN adjustment_group != 'not grouped' THEN adjustment_group
                    WHEN fg_group != 'not grouped' THEN fg_group
                    WHEN care_cost_group != 'not grouped' THEN care_cost_group
                    ELSE 'not grouped'
                END
            ) care_cost_reason_group
            ,CASE
                WHEN LOWER(COALESCE(adjustment_and_cancel_reason_combined, fg_reason, care_cost_reason)) IN (
                    'order eta update',
                    'delivery estimate confirmation',
                    'diner requested cancel - order too late',
                    'late delivery',
                    'late order',
                    'delivery estimate confirmation',
                    'order eta update',
                    'delivery estimate confirmation'
                ) THEN 'ETA Issues'
                ELSE 'Other'
            END AS eta_care_reasons
            ,COUNT(o3.order_uuid) orders
            ,COUNT(DISTINCT order_uuid) AS distinct_order_uuid
            ,SUM(total_care_cost) AS total_care_cost
            ,SUM(IF(ghd_ind = 'ghd', 1, 0)) ghd_orders
            ,SUM(IF((cp_diner_adj + cp_care_concession_awarded_amount + cp_care_ticket_cost) < 0, 1, 0)) orders_with_care_cost
            ,COUNT(
                CASE
                    WHEN o3.order_status_cancel_ind = TRUE THEN o3.order_uuid
                END
            ) cancels_osmf_definition
        FROM
            o3
        GROUP BY
            1,
            2,
            3
    )
