---Tableau: https://tableau.grubhub.com/#/site/Care/workbooks/6887/views
---Query Last Updated: 4.28.2025

WITH adj as(
SELECT ar.order_uuid
     , MAX_BY(reason,adjustment_timestamp_utc) AS adjustment_reason_name -- latest adjustment (on the off chance that there are multiple, won't duplicate order count when joining to order_fact/profit_fact)
     , MAX_BY(COALESCE(sr.name,pr.name),adjustment_timestamp_utc) AS adj_contact_reason
FROM source_cass_rainbow_data.adjustment_reporting AS ar
LEFT JOIN integrated_core.ticket_fact AS tf 
ON tf.ticket_id = ar.ticket_id 
   AND tf.ticket_created_date >= CURRENT_DATE - INTERVAL '6' MONTH
LEFT JOIN source_zendesk_ref.primary_contact_reason AS pr 
ON pr.contact_reason_id = tf.primary_contact_reason
LEFT JOIN source_zendesk_ref.secondary_contact_reason AS sr 
ON sr.contact_reason_id = tf.secondary_contact_reason
WHERE DATE(DATE_PARSE(CAST(adjustment_dt AS VARCHAR), '%Y%m%d')) >= CURRENT_DATE - INTERVAL '6' MONTH
   AND direction = 'ADJUST_DOWN' -- refund
   AND payer = 'GRUBHUB' -- paid by grubhub
GROUP BY 1
),


ghg AS(
SELECT cart_uuid AS order_uuid
     , max(CASE WHEN claim_type = 'SERVICE' THEN 'Late Delivery - GHG' 
                WHEN claim_type = 'PRICING' THEN 'Price - GHG' END) AS fg_reason
FROM ods.carecontactfacade_guarantee_claim
WHERE created_date >= CURRENT_DATE - INTERVAL '6' MONTH
   AND decision = 'GRANT'
GROUP BY 1
),


care_fg AS(
SELECT cr.order_uuid
     , MAX_BY(COALESCE(sr.name,pr.name),issue_timestamp_utc) AS fg_reason
FROM source_cass_rainbow_data.concession_reporting AS cr
LEFT JOIN integrated_core.ticket_fact AS tf 
ON tf.ticket_id = cr.ticket_id 
   AND tf.ticket_created_date >= CURRENT_DATE - INTERVAL '6' MONTH
LEFT JOIN source_zendesk_ref.primary_contact_reason AS pr 
ON pr.contact_reason_id = tf.primary_contact_reason
LEFT JOIN source_zendesk_ref.secondary_contact_reason AS sr 
ON sr.contact_reason_id = tf.secondary_contact_reason
WHERE DATE(DATE_PARSE(CAST(expiration_dt AS VARCHAR), '%Y%m%d')) >= CURRENT_DATE - INTERVAL '6' MONTH
GROUP BY 1
),


diner_ss_cancels AS(
SELECT order_id AS order_uuid
     , MAX(reason_code) AS reason_code
     , MAX(CASE WHEN reason_code = 'DINER_PAYMENT_ISSUE' THEN 'Payment Issues'
                WHEN reason_code = 'DINER_CHOSE_WRONG_ADDRESS' THEN 'Delivery Information Incorrect'
                WHEN reason_code = 'DINER_CHOSE_WRONG_ORDER_ITEMS' THEN 'Ordered By Mistake'
                WHEN reason_code = 'DINER_DOES_NOT_WANT_LATE_ORDER' THEN 'Late Order'
                WHEN reason_code = 'DINER_DOES_NOT_WANT_THE_FOOD' THEN 'Change of Plans'
            END) AS diner_ss_cancel_reason
     , MAX(CASE WHEN reason_code = 'DINER_PAYMENT_ISSUE' THEN 'Diner Issues'
                WHEN reason_code = 'DINER_CHOSE_WRONG_ADDRESS' THEN 'Diner Issues'
                WHEN reason_code = 'DINER_CHOSE_WRONG_ORDER_ITEMS' THEN 'Diner Issues'
                WHEN reason_code = 'DINER_DOES_NOT_WANT_LATE_ORDER' THEN 'Logistics Issues'
                WHEN reason_code = 'DINER_DOES_NOT_WANT_THE_FOOD' THEN 'Diner Issues'
            END) AS diner_ss_cancel_reason_group
FROM ods.carereporting_cancellation_result
WHERE created_date >= CURRENT_DATE - INTERVAL '6' MONTH
GROUP BY 1
),


cancels AS(
SELECT ocf.order_uuid
     , order_status_cancel_ind
     , CASE WHEN cancel_group = 'Not Mapped' AND diner_ss_cancel_reason_group IS NOT NULL THEN diner_ss_cancel_reason_group
            WHEN LOWER(order_status_cancel_reason) LIKE '%restaurant did%nt receive%' THEN 'Transmission Issues'
            ELSE cancel_group END AS cancel_group
     , CASE WHEN cancel_reason_name = 'Not Mapped' AND diner_ss_cancel_reason IS NOT NULL THEN diner_ss_cancel_reason
            WHEN LOWER(order_status_cancel_reason) LIKE '%restaurant did%nt receive%' THEN 'restaurant did not receive order'
            ELSE cancel_reason_name END AS cancel_reason_name
     , pr.name AS cancel_pcr
     , sr.name AS cancel_scr
     , COALESCE(cancellation_time_utc,tf.created_time) AS cancel_time_utc
     , COALESCE(sr.name,pr.name) AS cancel_contact_reason
FROM integrated_order.order_cancellation_fact AS ocf
LEFT JOIN integrated_ref.cancellation_reason_map AS crm 
ON CAST(crm.cancel_reason_id AS varchar) = CAST(ocf.primary_cancel_reason_id AS varchar)
LEFT JOIN integrated_core.ticket_fact AS tf 
ON tf.ticket_id = CAST(ocf.cancellation_ticket_id AS bigint) 
   AND tf.ticket_created_Date >= CURRENT_DATE - INTERVAL '6' MONTH
LEFT JOIN source_zendesk_ref.primary_contact_reason AS pr 
ON pr.contact_reason_id = tf.primary_contact_reason
LEFT JOIN source_zendesk_ref.secondary_contact_reason AS sr 
ON sr.contact_reason_id = tf.secondary_contact_reason
LEFT JOIN diner_ss_cancels AS ccr 
ON ccr.order_uuid = ocf.order_uuid
WHERE cancellation_date >= CURRENT_DATE - INTERVAL '6' MONTH
),


osmf AS (
SELECT order_uuid
     , status_cancelled_reached_ind
FROM integrated_order.order_status_milestone_fact
WHERE business_day >= CURRENT_DATE - INTERVAL '6' MONTH
),


of AS (
SELECT of.order_uuid
     , of.modified_cbsa_name
     , CASE WHEN of.modified_cbsa_name in ('New York CBSA Excluding Manhattan', 'New York - Manhattan', 'Chicago-Naperville-Elgin IL-IN-WI') THEN of.modified_cbsa_name
            ELSE 'Other CBSA'
       END AS key_cities_cbsa
    , irmd.ghd_delivery_region_name
    , CASE WHEN food_and_beverage > 1000 then 'Over $1000'
                   WHEN food_and_beverage > 250 then 'Over $250'
              ELSE 'Less than $250' END AS large_order_ind
FROM integrated_core.order_fact AS of
LEFT JOIN integrated_restaurant.merchant_dim AS irmd
ON of.cust_id = irmd.cust_id
WHERE DATE(of.delivery_time_ct) >= CURRENT_DATE - INTERVAL '6' MONTH
),


mdf AS(
SELECT order_uuid
     , MIN_BY(dropoff_complete_time_utc,order_created_time_utc) AS dropoff_complete_time_utc
     , MIN_BY(eta_at_order_placement_time_utc + INTERVAL '10' MINUTE,order_created_time_utc) AS ghd_eta_utc
     , CASE WHEN DATE_DIFF('minute',MIN_BY(eta_at_order_placement_time_utc + INTERVAL '10' MINUTE,order_created_time_utc),MIN_BY(dropoff_complete_time_utc,order_created_time_utc)) > 0 THEN 1 ELSE 0 END AS ghd_late_ind
     , MIN_BY(IF(bundle_type IS NOT NULL,true,false),order_created_time_utc) AS bundle_ind
     , MIN_BY(IF(delivery_fulfillment_type = 'SHOP_AND_PAY', true, false),order_created_time_utc) AS shop_and_pay_ind 
FROM integrated_delivery.managed_delivery_fact_v2
WHERE business_day >= CURRENT_DATE - INTERVAL '6' MONTH
GROUP BY 1
),
-- cost and speed main impact: late order, late delivery, food temperature, order unwanted - too late

rest_refunds AS (
SELECT order_uuid
     , SUM(net_amount) * 0.01 AS rr_refund_total
FROM ods.transaction 
WHERE created_date >= CURRENT_DATE - INTERVAL '6' MONTH
   AND transaction_time_ct >= CURRENT_DATE - INTERVAL '6' MONTH
   AND transaction_type = 'PCI_SINGLE_REFUND'
 GROUP BY 1
),


contacts AS(
SELECT order_uuid
     , MAX_BY(ticket_id, created_time) AS latest_ticket_id
     , MAX_BY(COALESCE(sr.name, pr.name), created_time) AS latest_contact_reason
     , MAX_BY(automation_deflected_ind, created_time) AS automated_ind
     , COUNT(ticket_id) AS contacts
FROM integrated_core.ticket_fact AS tf
LEFT JOIN source_zendesk_ref.primary_contact_reason AS pr 
ON pr.contact_reason_id = tf.primary_contact_reason
LEFT JOIN source_zendesk_ref.secondary_contact_reason AS sr 
ON sr.contact_reason_id = tf.secondary_contact_reason
WHERE ticket_created_date >= CURRENT_DATE - INTERVAL '6' MONTH
   AND ticket_created_date < CURRENT_DATE
   AND order_uuid IS NOT NULL
   AND cpo_contact_indicator = 1
GROUP BY 1
),


o AS(
SELECT DATE(cpf.delivery_time_ct) AS date1
     , IF(cpf.managed_delivery_ind=true,'ghd','non-ghd') AS ghd_ind
     , IF(cpf.delivery_ind=true,'delivery','pickup') AS delivery_ind
     , cpf.order_uuid
     , cp_diner_adj
     , COALESCE(c.order_status_cancel_ind,false) AS order_status_cancel_ind
     , IF(c.order_uuid IS NOT NULL,true,false) AS cancel_fact_ind
     , cancel_group
     , cancel_reason_name
     , cancel_contact_reason
     , osmf.status_cancelled_reached_ind
     , automated_ind
     , of.modified_cbsa_name
     , of.key_cities_cbsa
     , of.ghd_delivery_region_name
     , of.large_order_ind
     , CASE WHEN m.csa_name = 'New York-Newark - NY-NJ-CT-PA' THEN 'Tri-State' ELSE 'Other' END AS tri_state_ind
     , cpf.cancelled_order_ind
     , cpf.cp_revenue
     , COALESCE(ghd_late_ind,0) AS ghd_late_ind
     , CASE WHEN ghd_late_ind = 1 THEN 1 
            WHEN cancel_time_utc > ghd_eta_utc AND dropoff_complete_time_utc IS NULL THEN 1 
            ELSE 0 
       END AS ghd_late_ind_incl_cancel_time -- cancelled, no dropoff complete time
     , CASE WHEN REGEXP_LIKE(LOWER(adjustment_reason_name),'food temp|cold|quality_temp|temperature') THEN 'food temperature'
            WHEN REGEXP_LIKE(LOWER(adjustment_reason_name),'incorrect order|incorrect item|wrong order|incorrect_item') THEN 'incorrect order'
            WHEN REGEXP_LIKE(LOWER(adjustment_reason_name),'damaged') THEN 'food damaged'  
            WHEN REGEXP_LIKE(LOWER(adjustment_reason_name),'missing') THEN 'missing item'  
            WHEN REGEXP_LIKE(LOWER(adjustment_reason_name),'item removed') THEN 'item removed from order' 
            WHEN REGEXP_LIKE(LOWER(adjustment_reason_name),'late') THEN 'late order' 
            WHEN REGEXP_LIKE(LOWER(adjustment_reason_name),'menu error') THEN 'order or menu issue' 
            WHEN REGEXP_LIKE(LOWER(adjustment_reason_name),'temporarily unavailable|unavailable') THEN 'out of item'
            WHEN REGEXP_LIKE(LOWER(adjustment_reason_name),'order not rec|missed delivery') THEN 'missed delivery' 
            WHEN REGEXP_LIKE(LOWER(COALESCE(cancel_contact_reason,adj_contact_reason)),'food temp|cold|quality_temp|temperature') THEN 'food temperature'
            WHEN REGEXP_LIKE(LOWER(COALESCE(cancel_contact_reason,adj_contact_reason)),'incorrect order|incorrect item|wrong order|incorrect_item') THEN 'incorrect order'
            WHEN REGEXP_LIKE(LOWER(COALESCE(cancel_contact_reason,adj_contact_reason)),'damaged') THEN 'food damaged'  
            WHEN REGEXP_LIKE(LOWER(COALESCE(cancel_contact_reason,adj_contact_reason)),'missing') THEN 'missing item'  
            WHEN REGEXP_LIKE(LOWER(COALESCE(cancel_contact_reason,adj_contact_reason)),'item removed') THEN 'item removed from order' 
            WHEN REGEXP_LIKE(LOWER(COALESCE(cancel_contact_reason,adj_contact_reason)),'late') THEN 'late order' 
            WHEN REGEXP_LIKE(LOWER(COALESCE(cancel_contact_reason,adj_contact_reason)),'menu error') THEN 'order or menu issue' 
            WHEN REGEXP_LIKE(LOWER(COALESCE(cancel_contact_reason,adj_contact_reason)),'temporarily unavailable|unavailable') THEN 'out of item'
            WHEN REGEXP_LIKE(LOWER(COALESCE(cancel_contact_reason,adj_contact_reason)),'order not rec|missed delivery') THEN 'missed delivery' 
            when REGEXP_LIKE(LOWER(adjustment_reason_name),'refund due to|refund for') THEN LOWER(COALESCE(cancel_contact_reason,adj_contact_reason,adjustment_reason_name))
            WHEN REGEXP_LIKE(LOWER(COALESCE(cancel_contact_reason,adj_contact_reason)),'refund due to|refund for') THEN LOWER(COALESCE(cancel_contact_reason,adj_contact_reason,adjustment_reason_name))
            ELSE LOWER(COALESCE(cancel_contact_reason,adj_contact_reason,adjustment_reason_name)) -- use contact reason when there's a long reason (refund due to item 1 item 2 etc)
       END AS adjustment_reason_name
     , adj_contact_reason
     , tip
     , bundle_ind
     , mdf.shop_and_pay_ind
     , CASE WHEN cp_care_concession_awarded_amount = 0 THEN NULL -- no fg in profit fact
            WHEN REGEXP_LIKE(LOWER(COALESCE(ghg.fg_reason,care_fg.fg_reason)),'food temp|cold|quality_temp|temperature') THEN 'food temperature'
            WHEN REGEXP_LIKE(LOWER(COALESCE(ghg.fg_reason,care_fg.fg_reason)),'incorrect order|incorrect item|wrong order|incorrect_item') THEN 'incorrect order'
            WHEN REGEXP_LIKE(LOWER(COALESCE(ghg.fg_reason,care_fg.fg_reason)),'damaged') THEN 'food damaged'  
            WHEN REGEXP_LIKE(LOWER(COALESCE(ghg.fg_reason,care_fg.fg_reason)),'missing') THEN 'missing item'  
            WHEN REGEXP_LIKE(LOWER(COALESCE(ghg.fg_reason,care_fg.fg_reason)),'item removed') THEN 'item removed from order' 
            WHEN REGEXP_LIKE(LOWER(COALESCE(ghg.fg_reason,care_fg.fg_reason)),'late') THEN 'late order' 
            WHEN REGEXP_LIKE(LOWER(COALESCE(ghg.fg_reason,care_fg.fg_reason)),'menu error') THEN 'order or menu issue' 
            WHEN REGEXP_LIKE(LOWER(COALESCE(ghg.fg_reason,care_fg.fg_reason)),'temporarily unavailable|unavailable') THEN 'out of item'
            WHEN REGEXP_LIKE(LOWER(COALESCE(ghg.fg_reason,care_fg.fg_reason)),'order not rec|missed delivery') THEN 'missed delivery' 
            ELSE LOWER(COALESCE(ghg.fg_reason,care_fg.fg_reason)) 
            END AS fg_reason
     , ccr.care_cost_reason -- excludes contact reasons where we typically give refunds/freegrub, as reasons will be captured by fg reason and adj reason logic. If we change the logic, use contacts.latest_contact_reason
     , COALESCE(ccr.care_cost_group,'not grouped') AS care_cost_group  -- excludes contact reasons where we typically give refunds/freegrub, as reasons will be captured by fg reason and adj reason logic          
     , cp_care_concession_awarded_amount AS cp_care_concession_awarded_amount
     , COALESCE(cp_driver_pay_per_order,0) AS driver_pay_per_order
     , COALESCE(cp_driver_true_up,0) AS true_up
     , COALESCE(cp_redelivery_cost,0) AS cp_redelivery_cost
     , COALESCE(cp_grubcash_care_concession_awarded_amount,0) AS cp_grubcash_care_concession_awarded_amount
     , COALESCE(cp_grub_care_refund,0) AS cp_grub_care_refund
     , COALESCE(rr.rr_refund_total,0) AS rr_refund
     , cp_diner_care_tickets+cp_driver_care_tickets+cp_restaurant_care_tickets+cp_gh_internal_care_tickets AS cp_care_ticket_cost

FROM integrated_order.order_contribution_profit_fact AS cpf
LEFT JOIN cancels AS c 
ON c.order_uuid = cpf.order_uuid
LEFT JOIN adj 
ON adj.order_uuid = cpf.order_uuid
LEFT JOIN mdf 
ON mdf.order_uuid = cpf.order_uuid
LEFT JOIN ghg 
ON ghg.order_uuid = cpf.order_uuid
LEFT JOIN care_fg 
ON care_fg.order_uuid = cpf.order_uuid
LEFT JOIN contacts
ON contacts.order_uuid = cpf.order_uuid
LEFT JOIN rest_refunds AS rr
ON rr.order_uuid = cpf.order_uuid
LEFT JOIN osmf
ON osmf.order_uuid = cpf.order_uuid
LEFT JOIN of
ON of.order_uuid = cpf.order_uuid
LEFT JOIN csv_sandbox.care_cost_reasons AS ccr 
ON ccr.scr = contacts.latest_contact_reason
LEFT JOIN integrated_geo.order_location AS g 
ON of.order_uuid = g.order_uuid
LEFT JOIN integrated_geo.blockgroup_dim AS m 
ON m.blockgroup_geoid = g.dropoff_blockgroup_geoid
  AND substr(m.blockgroup_geoid,12,1)<>'0'
WHERE cpf.order_date >= CURRENT_DATE - INTERVAL '6' MONTH
   AND DATE(cpf.delivery_time_ct) >= CURRENT_DATE - INTERVAL '6' MONTH
   AND DATE(cpf.delivery_time_ct) < CURRENT_DATE
),


o2 AS(
SELECT date1
     , ghd_ind
     , delivery_ind
     , order_uuid
     , cp_diner_adj
     , order_status_cancel_ind
     , automated_ind
     , cancel_fact_ind
     , cancel_group
     , cancel_reason_name
     , cancel_contact_reason
     , status_cancelled_reached_ind
     , modified_cbsa_name
     , key_cities_cbsa
     , ghd_delivery_region_name
     , large_order_ind
     , tri_state_ind
     , cancelled_order_ind
     , cp_revenue
     , ghd_late_ind
     , ghd_late_ind_incl_cancel_time
     , CASE WHEN cancel_reason_name = 'Not Mapped' THEN adjustment_reason_name
            WHEN cancel_reason_name IS NULL THEN adjustment_reason_name
            ELSE LOWER(cancel_reason_name) 
        END AS adjustment_and_cancel_reason_combined
     , fg_reason
     , cp_care_concession_awarded_amount
     , driver_pay_per_order
     , true_up
     , tip
     , bundle_ind
     , shop_and_pay_ind
     , cp_care_ticket_cost
     , care_cost_reason 
     , care_cost_group
     , cp_redelivery_cost
     , cp_grubcash_care_concession_awarded_amount
     , cp_grub_care_refund
     , rr_refund
FROM o
),


o3 AS(
SELECT date1
     , ghd_ind
     , delivery_ind
     , order_uuid
     , cp_diner_adj
     , order_status_cancel_ind
     , automated_ind
     , cancel_fact_ind
     , o2.cancel_group
     , o2.cancel_reason_name
     , cancel_contact_reason
     , ghd_late_ind
     , ghd_late_ind_incl_cancel_time
     , CASE WHEN o2.cancel_group IS NOT NULL AND o2.cancel_group != 'Other' THEN crm.cancel_group
            WHEN REGEXP_LIKE(LOWER(adjustment_and_cancel_reason_combined),'missed delivery|order not rec|late|damaged|ghd driver|food temp|quality_temp|cold') THEN 'Logistics Issues'
            WHEN REGEXP_LIKE(LOWER(adjustment_and_cancel_reason_combined),'missing item|missing|incorrect_order|menu error|incorrect_item|missing_item|incorrect item|incorrect order|quality|special|problem with food|food poison|object in food|temporarily unavailable|out of item|item removed|foreign object') THEN 'Restaurant Issues'
            WHEN REGEXP_LIKE(LOWER(adjustment_and_cancel_reason_combined),'diner error|switch to delivery or pickup|change of plans') THEN 'Diner Issues'
            ELSE COALESCE(crm.cancel_group,'not grouped') 
        END AS adjustment_group
     , CASE WHEN o2.cancel_group IS NOT NULL AND o2.cancel_group != 'Other' THEN crm.cancel_group
            WHEN REGEXP_LIKE(LOWER(fg_reason),'missed delivery|order not rec|late|damaged|ghd driver|food temp|quality_temp|cold') THEN 'Logistics Issues'
            WHEN REGEXP_LIKE(LOWER(fg_reason),'missing item|missing|incorrect_order|menu error|incorrect_item|missing_item|incorrect item|incorrect order|quality|special|problem with food|food poison|object in food|temporarily unavailable|out of item|item removed|foreign object') THEN 'Restaurant Issues'
            WHEN REGEXP_LIKE(LOWER(fg_reason),'diner error|switch to delivery or pickup|change of plans') THEN 'Diner Issues'
            ELSE COALESCE(crm.cancel_group,'not grouped') 
        END AS fg_group
     , CASE WHEN cp_care_concession_awarded_amount < 0 AND fg_reason IS NULL THEN adjustment_and_cancel_reason_combined
            ELSE fg_reason 
        END AS fg_reason
     , cp_care_concession_awarded_amount
     , adjustment_and_cancel_reason_combined
     , status_cancelled_reached_ind
     , modified_cbsa_name
     , key_cities_cbsa
     , ghd_delivery_region_name
     , large_order_ind
     , tri_state_ind
     , cancelled_order_ind
     , cp_revenue
     , driver_pay_per_order
     , tip
     , true_up
     , bundle_ind
     , shop_and_pay_ind
     , cp_care_ticket_cost
     , care_cost_reason 
     , care_cost_group
     , cp_redelivery_cost
     , cp_grubcash_care_concession_awarded_amount
     , cp_grub_care_refund
     , rr_refund

FROM o2
LEFT JOIN integrated_ref.cancellation_reason_map AS crm 
ON LOWER(crm.cancel_reason_name) = LOWER(o2.adjustment_and_cancel_reason_combined) 
)


SELECT date1
     , ghd_ind
     , delivery_ind
     , cancel_fact_ind
     , adjustment_group
     , adjustment_and_cancel_reason_combined
     , ghd_late_ind
     , ghd_late_ind_incl_cancel_time
     , bundle_ind
     , shop_and_pay_ind
     , automated_ind
     , modified_cbsa_name
     , key_cities_cbsa
     , ghd_delivery_region_name
     , large_order_ind
     , tri_state_ind
     , LOWER(CASE WHEN (cp_diner_adj+cp_care_concession_awarded_amount+cp_care_ticket_cost+cp_redelivery_cost+cp_grub_care_refund) = 0 then 'orders with no care cost' -- orders with no care cost
                  WHEN cp_redelivery_cost < 0 THEN 'logistics issues'
                  WHEN adjustment_group != 'not grouped' THEN adjustment_group
                  WHEN fg_group != 'not grouped' THEN fg_group
                  WHEN care_cost_group != 'not grouped' THEN care_cost_group
                  ELSE 'not grouped' 
            END) AS care_cost_reason_group
     , CASE WHEN cp_redelivery_cost<0 THEN 'Missed Delivery' 
            ELSE LOWER(COALESCE(adjustment_and_cancel_reason_combined,fg_reason,care_cost_reason)) 
        END AS care_cost_reason
     , COUNT(DISTINCT o3.order_uuid) AS orders
     , SUM(IF(cancel_fact_ind=true,1,0)) AS cancels
     , SUM(CASE WHEN status_cancelled_reached_ind = TRUE AND cancelled_order_ind = FALSE THEN cp_revenue END) AS missed_revenue_cp
     , SUM(cp_diner_adj) AS cp_diner_adj
     , SUM(cp_care_concession_awarded_amount) AS cp_care_concession_awarded_amount
     , SUM(cp_care_ticket_cost) AS cp_care_ticket_cost
     , SUM(cp_redelivery_cost) AS redelivery_cost
     , SUM(cp_grubcash_care_concession_awarded_amount) AS cp_grubcash_care_concession_awarded_amount
     , SUM(cp_grub_care_refund) AS cp_grubcare_refund
     , SUM(rr_refund) AS rr_refund
     , SUM(cp_diner_adj+cp_care_concession_awarded_amount+cp_care_ticket_cost+cp_redelivery_cost+cp_grub_care_refund) AS cp_total_care_cost
     , SUM(IF(ghd_late_ind=1,1,0)) AS ghd_late_count
     , SUM(IF(ghd_ind='ghd',1,0)) AS ghd_orders
     , SUM(driver_pay_per_order) driver_pay_cpf
     , SUM(tip) AS tip
     , SUM(true_up) AS true_up
     , SUM(IF(cp_diner_adj<0,1,0)) AS orders_with_adjustments
     , SUM(IF(cp_care_concession_awarded_amount<0,1,0)) AS orders_with_fg
     , SUM(IF(cp_redelivery_cost<0,1,0)) AS orders_with_redelivery
     , SUM(IF(cp_grubcash_care_concession_awarded_amount<0,1,0)) AS orders_with_gh_credit
     , SUM(IF(cp_grub_care_refund<0,1,0)) AS orders_with_gh_credit_refund
     , SUM(IF(ABS(rr_refund)>0,1,0)) AS orders_with_rr_refund
     , SUM(CASE WHEN (cp_diner_adj+cp_care_concession_awarded_amount+cp_care_ticket_cost+cp_redelivery_cost+cp_grub_care_refund)<0 THEN 1 
                ELSE 0
            END) AS orders_with_care_cost
    , COUNT(CASE WHEN o3.order_status_cancel_ind = true THEN o3.order_uuid END) AS cancels_osmf_definition
FROM o3
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18