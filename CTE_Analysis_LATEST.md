# CTE Analysis - LATEST Fulfillment Care Cost Query

This document provides a detailed, column-level logical analysis of each Common Table Expression (CTE) in the LATEST_fulfillment_care_cost.sql query. The analysis is based solely on the SQL query and the context provided in "Breaking Down Logistic Care Costs Query.md".

## Analysis Overview
The LATEST query processes order fulfillment care costs through 13 CTEs and a final aggregation that progressively build upon each other. The main data flow is: source data extraction → reason standardization → data integration → cost calculation → final aggregation. Key business logic includes extensive regex-based reason categorization and hierarchical cost attribution.

**Key Differences from Original Query:**
- Uses a rolling 6-month date window (`CURRENT_DATE - INTERVAL '6' MONTH`) instead of parameterized start/end dates
- Includes new CTEs: `osmf` (order status milestone fact), `of` (order fact with CBSA data), and `rest_refunds` (restaurant refunds)
- Adds new market segmentation dimensions: `modified_cbsa_name`, `key_cities_cbsa`, `ghd_delivery_region_name`, `tri_state_ind`
- Includes additional financial components: `cp_grubcash_care_concession_awarded_amount`, `rr_refund`, `true_up`
- Adds `shop_and_pay_ind` indicator for Shop and Pay orders
- Includes `delivery_ind` to distinguish delivery vs pickup orders
- Adds `automated_ind` to track automation-deflected contacts
- Different lateness calculation using `MIN_BY` aggregation
- Expanded final output metrics with more granular cost breakdowns

**CTE Dependency Flow:**
1. **Data Extraction Layer:** adj, ghg, care_fg, diner_ss_cancels, osmf, mdf, rest_refunds, contacts (independent source data pulls)
2. **Integration Layer:** cancels (uses diner_ss_cancels), of (joins order fact with merchant data)
3. **Main Integration:** o (joins all previous CTEs with financial data)
4. **Transformation Layer:** o2 (reason consolidation), o3 (cost calculation & final groupings)
5. **Aggregation Layer:** Final SELECT (business metrics rollup)

**Critical Data Quality Patterns Identified:**
- Extensive use of MAX_BY and MIN_BY functions suggests potential duplicate records in source tables requiring consistent selection logic
- Multiple COALESCE and fallback logic patterns indicate missing data handling is a significant concern
- Regex pattern duplication across CTEs creates maintenance risks
- Hierarchical reason attribution may mask underlying data quality issues
- Rolling 6-month window requires careful consideration of data completeness and historical trend analysis

**Performance and Complexity Considerations:**
- Rolling 6-month date filters across multiple CTEs may impact query performance depending on data volume
- Complex regex matching in o CTE applied to large datasets may impact performance
- Multiple LEFT JOINs in primary integration CTE (o) could benefit from indexing analysis
- Hardcoded UUID and value lists in market segmentation create maintenance overhead
- Extensive CASE statement logic suggests potential for lookup table optimization
- MIN_BY aggregation in mdf CTE may have performance implications on large datasets

**Business Stakeholder Priorities:**
- Reason standardization logic requires ongoing maintenance as new reason types emerge
- Market segmentation includes multiple dimensions (CBSA, regions, tri-state) requiring coordination
- Cost attribution hierarchy may need periodic review for business alignment
- Automation deflection tracking suggests focus on operational efficiency metrics
- Shop and Pay indicator suggests specific tracking for this business line
- Restaurant refund tracking indicates multi-party cost attribution concerns

## CTE: adj
Purpose: Identifies orders with Grubhub-paid refunds and retrieves the latest adjustment reason and associated contact reason for each order.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| order_uuid | source_cass_rainbow_data.adjustment_reporting.order_uuid | Direct selection from the source table | None |
| adjustment_reason_name | source_cass_rainbow_data.adjustment_reporting.reason | Uses MAX_BY function to get the reason corresponding to the latest adjustment_timestamp_utc for each order | Question: The use of MAX_BY suggests multiple adjustment records per order are possible. What is the frequency of multiple adjustments per order, and could this aggregation be masking important business insights about repeated adjustment patterns? |
| adj_contact_reason | source_zendesk_ref.secondary_contact_reason.name, source_zendesk_ref.primary_contact_reason.name | Uses MAX_BY with COALESCE(sr.name, pr.name) to get the contact reason corresponding to the latest adjustment_timestamp_utc, prioritizing secondary over primary | Question: What is the business rationale for prioritizing secondary contact reason over primary contact reason? Is this hierarchy documented in business requirements? |

**Date Filtering Logic:**
- adjustment_reporting filtered by: `DATE(DATE_PARSE(CAST(adjustment_dt AS VARCHAR), '%Y%m%d')) >= CURRENT_DATE - INTERVAL '6' MONTH`
- ticket_fact filtered by: `tf.ticket_created_date >= CURRENT_DATE - INTERVAL '6' MONTH`
- Question: The rolling 6-month window means the dataset continuously changes. How does this impact historical analysis and trend tracking? Are there mechanisms to ensure consistency when comparing reports from different dates?

**Filter Conditions:**
- `direction = 'ADJUST_DOWN'` - ensures only refunds are captured
- `payer = 'GRUBHUB'` - ensures only Grubhub-paid adjustments

## CTE: ghg  
Purpose: Identifies orders with granted Grubhub Guarantee claims and categorizes them into standardized reason types.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| order_uuid | ods.carecontactfacade_guarantee_claim.cart_uuid | Direct selection from source, with alias cart_uuid mapped to order_uuid | Question: Is the cart_uuid to order_uuid mapping always 1:1? Are there any scenarios where this relationship might differ? |
| fg_reason | ods.carecontactfacade_guarantee_claim.claim_type | CASE statement with MAX aggregation that maps 'SERVICE' → 'Late Delivery - GHG' and 'PRICING' → 'Price - GHG' | Question: What happens to guarantee claims with claim_type values other than 'SERVICE' or 'PRICING'? Are there other claim types in the data that should be handled? |

**Date Filtering Logic:**
- Filtered by: `created_date >= CURRENT_DATE - INTERVAL '6' MONTH`

**Filter Conditions:**
- `decision = 'GRANT'` - only includes granted claims, excluding denied or pending

## CTE: care_fg
Purpose: Identifies orders that had concessions and retrieves the latest contact reason associated with those concessions.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| order_uuid | source_cass_rainbow_data.concession_reporting.order_uuid | Direct selection from the source table | None |
| fg_reason | source_zendesk_ref.secondary_contact_reason.name, source_zendesk_ref.primary_contact_reason.name | Uses MAX_BY with COALESCE(sr.name, pr.name) to get the contact reason corresponding to the latest issue_timestamp_utc | Question: This follows the same secondary-over-primary prioritization pattern as other CTEs. Is this a consistent business rule across all care interactions, and are there any scenarios where primary contact reason should take precedence? |

**Date Filtering Logic:**
- concession_reporting filtered by: `DATE(DATE_PARSE(CAST(expiration_dt AS VARCHAR), '%Y%m%d')) >= CURRENT_DATE - INTERVAL '6' MONTH`
- ticket_fact filtered by: `tf.ticket_created_date >= CURRENT_DATE - INTERVAL '6' MONTH`

*Note: This CTE focuses specifically on concessions (free grub) and links them to care contact reasons, providing the foundation for fg_reason attribution in downstream analysis.*

## CTE: diner_ss_cancels
Purpose: Identifies orders with diner self-service cancellations and maps reason codes to standardized descriptions and groups.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| order_uuid | ods.carereporting_cancellation_result.order_id | Direct selection with alias order_id mapped to order_uuid | None |
| reason_code | ods.carereporting_cancellation_result.reason_code | Uses MAX aggregation to select a representative reason_code per order | Question: Why use MAX aggregation instead of latest by timestamp or most frequent reason? If multiple reason codes exist per order, what is the business justification for this selection method? |
| diner_ss_cancel_reason | ods.carereporting_cancellation_result.reason_code | CASE statement mapping: 'DINER_PAYMENT_ISSUE' → 'Payment Issues', 'DINER_CHOSE_WRONG_ADDRESS' → 'Delivery Information Incorrect', 'DINER_CHOSE_WRONG_ORDER_ITEMS' → 'Ordered By Mistake', 'DINER_DOES_NOT_WANT_LATE_ORDER' → 'Late Order', 'DINER_DOES_NOT_WANT_THE_FOOD' → 'Change of Plans' | Question: Is this the complete set of possible reason_code values in the source data? What happens to unmapped reason codes - are they set to NULL or excluded? |
| diner_ss_cancel_reason_group | ods.carereporting_cancellation_result.reason_code | CASE statement mapping reason codes to two groups: 'Diner Issues' (payment, wrong address, wrong items, change of plans) and 'Logistics Issues' (late order only) | Question: The mapping classifies most cancellations as 'Diner Issues' except lateness. How does this align with the overall fulfillment cost attribution framework used elsewhere in the query? |

**Date Filtering Logic:**
- Filtered by: `created_date >= CURRENT_DATE - INTERVAL '6' MONTH`

## CTE: cancels
Purpose: Identifies cancelled orders and consolidates cancellation information from multiple sources including self-service cancellations.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| order_uuid | integrated_order.order_cancellation_fact.order_uuid | Direct selection from the primary source table | None |
| order_status_cancel_ind | integrated_order.order_cancellation_fact.order_status_cancel_ind | Direct selection from the source table | Question: What does this indicator represent specifically? How does it differ from the existence of a record in the cancellation fact table? |
| cancel_group | integrated_ref.cancellation_reason_map.cancel_group, diner_ss_cancels.diner_ss_cancel_reason_group | Complex CASE logic that prioritizes diner self-service reasons when main cancel_group is 'Not Mapped', includes hardcoded logic for restaurant transmission issues | Question: The hardcoded pattern '%restaurant did%nt receive%' seems very specific. Is this documented business logic or ad-hoc fix? Can this pattern matching be clarified? |
| cancel_reason_name | integrated_ref.cancellation_reason_map.cancel_reason_name, diner_ss_cancels.diner_ss_cancel_reason | Similar CASE logic as cancel_group, with fallback to diner self-service reasons and hardcoded restaurant transmission case | Question: Same concern about the hardcoded restaurant pattern. Is there a comprehensive mapping of all possible cancellation scenarios? |
| cancel_pcr | source_zendesk_ref.primary_contact_reason.name | Direct selection from primary contact reason reference table | None |
| cancel_scr | source_zendesk_ref.secondary_contact_reason.name | Direct selection from secondary contact reason reference table | None |
| cancel_time_utc | integrated_order.order_cancellation_fact.cancellation_time_utc, integrated_core.ticket_fact.created_time | Uses COALESCE to prefer cancellation_time_utc, falling back to ticket created_time | Question: Under what circumstances would cancellation_time_utc be null but created_time be available? What does this fallback represent? |
| cancel_contact_reason | source_zendesk_ref.secondary_contact_reason.name, source_zendesk_ref.primary_contact_reason.name | Uses COALESCE to prefer secondary over primary contact reason | Consistent with pattern seen in other CTEs |

**Date Filtering Logic:**
- order_cancellation_fact filtered by: `cancellation_date >= CURRENT_DATE - INTERVAL '6' MONTH`
- ticket_fact filtered by: `tf.ticket_created_Date >= CURRENT_DATE - INTERVAL '6' MONTH`

## CTE: osmf
Purpose: Retrieves order status milestone data to track cancellation status indicators.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| order_uuid | integrated_order.order_status_milestone_fact.order_uuid | Direct selection from the source table | None |
| status_cancelled_reached_ind | integrated_order.order_status_milestone_fact.status_cancelled_reached_ind | Direct selection from the source table | Question: What is the difference between this indicator and order_status_cancel_ind from the cancels CTE? When would these values differ, and what business scenarios do they represent? [Interpretation] This may track whether an order reached cancelled status in the order lifecycle, distinct from cancellation fact records. |

**Date Filtering Logic:**
- Filtered by: `business_day >= CURRENT_DATE - INTERVAL '6' MONTH`

*Note: This CTE is a new addition compared to the original query, providing additional cancellation status tracking from the order status milestone perspective.*

## CTE: of
Purpose: Retrieves order-level data including geographic market information (CBSA) and order size categorization.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| order_uuid | integrated_core.order_fact.order_uuid | Direct selection from the source table | None |
| modified_cbsa_name | integrated_core.order_fact.modified_cbsa_name | Direct selection from the source table | [Interpretation] CBSA (Core Based Statistical Area) represents metropolitan/micropolitan statistical areas |
| key_cities_cbsa | integrated_core.order_fact.modified_cbsa_name | CASE statement mapping specific CBSAs to themselves ('New York CBSA Excluding Manhattan', 'New York - Manhattan', 'Chicago-Naperville-Elgin IL-IN-WI'), else 'Other CBSA' | Question: This creates a key cities focus for analysis. Are these three CBSAs the primary markets of interest? Should this list be maintained in a reference table for flexibility? |
| ghd_delivery_region_name | integrated_restaurant.merchant_dim.ghd_delivery_region_name | Direct selection from merchant dimension via cust_id join | Question: Why is delivery region sourced from merchant dimension rather than order or delivery facts? Does this represent the merchant's operating region? |
| large_order_ind | integrated_core.order_fact.food_and_beverage | CASE statement: 'Over $1000' when > 1000, 'Over $250' when > 250, else 'Less than $250' | Question: Are these thresholds aligned with business definitions of large orders? Do different order sizes have different care cost patterns or operational challenges? |

**Date Filtering Logic:**
- Filtered by: `DATE(of.delivery_time_ct) >= CURRENT_DATE - INTERVAL '6' MONTH`

*Note: This CTE is a new addition compared to the original query, providing geographic market segmentation based on CBSA classifications and merchant delivery regions.*

## CTE: mdf
Purpose: Retrieves comprehensive delivery data for Grubhub-managed deliveries with derived lateness and operational indicators.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| order_uuid | integrated_delivery.managed_delivery_fact_v2.order_uuid | Direct selection from the source table | None |
| dropoff_complete_time_utc | integrated_delivery.managed_delivery_fact_v2.dropoff_complete_time_utc | Uses MIN_BY to select dropoff_complete_time_utc corresponding to earliest order_created_time_utc | Question: Why use MIN_BY with order_created_time_utc as the selector? This differs from the original query's approach. Are there multiple delivery records per order, and does this select the first delivery attempt? |
| ghd_eta_utc | integrated_delivery.managed_delivery_fact_v2.eta_at_order_placement_time_utc | Uses MIN_BY with 10-minute buffer added (eta_at_order_placement_time_utc + INTERVAL '10' MINUTE), selecting based on earliest order_created_time_utc | Question: Why exactly 10 minutes? Is this a standard buffer time for GHD deliveries? Does this buffer account for acceptable variance in delivery time? |
| ghd_late_ind | integrated_delivery.managed_delivery_fact_v2 (multiple fields) | CASE statement checking if DATE_DIFF in minutes between ghd_eta_utc and dropoff_complete_time_utc > 0, returns 1 if late else 0 | [Interpretation] This is the primary lateness indicator - orders are considered late if actual delivery exceeds ETA by more than 10 minutes |
| bundle_ind | integrated_delivery.managed_delivery_fact_v2.bundle_type | Uses MIN_BY to select boolean check of bundle_type IS NOT NULL, based on earliest order_created_time_utc | Question: What constitutes a bundle and what are the possible bundle_type values? Are bundled orders more prone to care issues? |
| shop_and_pay_ind | integrated_delivery.managed_delivery_fact_v2.delivery_fulfillment_type | Uses MIN_BY to select boolean check of delivery_fulfillment_type = 'SHOP_AND_PAY', based on earliest order_created_time_utc | [Interpretation] Shop and Pay indicates grocery or convenience store orders where driver shops for items. This is a new business line being tracked. |

**Date Filtering Logic:**
- Filtered by: `business_day >= CURRENT_DATE - INTERVAL '6' MONTH`

**Important Aggregation Pattern:**
- All fields use MIN_BY aggregation with order_created_time_utc as selector, grouped by order_uuid
- Question: This aggregation pattern suggests multiple records per order_uuid in managed_delivery_fact_v2. What scenarios create multiple records (e.g., redeliveries, failed delivery attempts)? How does selecting by earliest order_created_time ensure the correct record is chosen?

*Note: This CTE's MIN_BY aggregation approach differs significantly from the original query which selected various fields without aggregation, suggesting the LATEST query handles potential duplicate delivery records differently.*

## CTE: rest_refunds
Purpose: Calculates total restaurant-issued refunds for orders.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| order_uuid | ods.transaction.order_uuid | Direct selection from the source table | None |
| rr_refund_total | ods.transaction.net_amount | Sums net_amount and multiplies by 0.01 (converts cents to dollars) | [Interpretation] Restaurant refunds represent refunds issued by the restaurant rather than Grubhub, contributing to total care costs |

**Date Filtering Logic:**
- Double date filtering: `created_date >= CURRENT_DATE - INTERVAL '6' MONTH` AND `transaction_time_ct >= CURRENT_DATE - INTERVAL '6' MONTH`
- Question: Why filter on both created_date and transaction_time_ct? What scenarios would cause these to differ significantly?

**Filter Conditions:**
- `transaction_type = 'PCI_SINGLE_REFUND'` - ensures only refund transactions are captured

*Note: This CTE is a new addition compared to the original query, adding restaurant refunds as a separate cost component tracked in the analysis.*

## CTE: contacts
Purpose: Identifies orders with worked care contacts and retrieves the latest contact information and total contact counts.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| order_uuid | integrated_core.ticket_fact.order_uuid | Direct selection from the source table | None |
| latest_ticket_id | integrated_core.ticket_fact.ticket_id | Uses MAX_BY to get ticket_id corresponding to the latest created_time | None |
| latest_contact_reason | source_zendesk_ref.secondary_contact_reason.name, source_zendesk_ref.primary_contact_reason.name | Uses MAX_BY with COALESCE(sr.name, pr.name) to get contact reason corresponding to latest created_time | Consistent with pattern in other CTEs - secondary reason prioritized |
| automated_ind | integrated_core.ticket_fact.automation_deflected_ind | Uses MAX_BY to get automation_deflected_ind corresponding to the latest created_time | Question: This tracks whether the latest contact was deflected through automation. How does this relate to care costs - are automated contacts excluded from ticket costs? |
| contacts | integrated_core.ticket_fact.ticket_id | COUNT of ticket_id records for each order_uuid | Represents total number of care contacts for the order |

**Date Filtering Logic:**
- Filtered by: `ticket_created_date >= CURRENT_DATE - INTERVAL '6' MONTH` AND `ticket_created_date < CURRENT_DATE`
- Question: The exclusion of CURRENT_DATE (`< CURRENT_DATE`) suggests incomplete data for the current day. Is this a data latency consideration?

**Filter Conditions:**
- `order_uuid IS NOT NULL` - ensures contacts are linked to orders
- `cpo_contact_indicator = 1` - focuses only on "worked" contacts where care agents were involved

*Note: The addition of automated_ind is new compared to the original query, suggesting increased focus on tracking automation effectiveness.*

## CTE: o
Purpose: Integrates operational and financial data, standardizes issue reasons, and calculates specific costs for comprehensive order analysis.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| date1 | integrated_order.order_contribution_profit_fact.delivery_time_ct | DATE function applied to delivery_time_ct | Primary analytical date for the order |
| ghd_ind | integrated_order.order_contribution_profit_fact.managed_delivery_ind | IF statement converting boolean to 'ghd'/'non-ghd' text values | None |
| delivery_ind | integrated_order.order_contribution_profit_fact.delivery_ind | IF statement converting boolean to 'delivery'/'pickup' text values | Question: How do pickup orders generate fulfillment care costs? Are the cost patterns significantly different from delivery orders? |
| order_uuid | integrated_order.order_contribution_profit_fact.order_uuid | Direct selection from the source table | None |
| cp_diner_adj | integrated_order.order_contribution_profit_fact.cp_diner_adj | Direct selection from the source table | Diner adjustments cost component |
| order_status_cancel_ind | cancels.order_status_cancel_ind | Uses COALESCE to default to FALSE if null | None |
| cancel_fact_ind | cancels.order_uuid | IF statement checking if cancels.order_uuid IS NOT NULL | Boolean indicator of whether order appears in cancellation fact |
| cancel_group | cancels.cancel_group | Direct selection from cancels CTE | None |
| cancel_reason_name | cancels.cancel_reason_name | Direct selection from cancels CTE | None |
| cancel_contact_reason | cancels.cancel_contact_reason | Direct selection from cancels CTE | None |
| status_cancelled_reached_ind | osmf.status_cancelled_reached_ind | Direct selection from osmf CTE | Cancellation status from order status milestone |
| automated_ind | contacts.automated_ind | Direct selection from contacts CTE | Automation deflection indicator |
| modified_cbsa_name | of.modified_cbsa_name | Direct selection from of CTE | CBSA market identifier |
| key_cities_cbsa | of.key_cities_cbsa | Direct selection from of CTE | Key cities categorization |
| ghd_delivery_region_name | of.ghd_delivery_region_name | Direct selection from of CTE | Delivery region from merchant dimension |
| large_order_ind | of.large_order_ind | Direct selection from of CTE | Order size categorization |
| tri_state_ind | integrated_geo.blockgroup_dim.csa_name | CASE statement: 'Tri-State' when csa_name = 'New York-Newark - NY-NJ-CT-PA', else 'Other' | Question: Is Tri-State area a specific focus market for analysis? How does this overlap with other geographic segmentations (CBSA, regions)? |
| cancelled_order_ind | integrated_order.order_contribution_profit_fact.cancelled_order_ind | Direct selection from the source table | Question: How does this differ from order_status_cancel_ind and cancel_fact_ind? What are the scenarios where these indicators would have different values? |
| cp_revenue | integrated_order.order_contribution_profit_fact.cp_revenue | Direct selection from the source table | Revenue metric for analyzing revenue loss from cancellations |
| ghd_late_ind | mdf.ghd_late_ind | Uses COALESCE to default to 0 if null | Basic lateness indicator |
| ghd_late_ind_incl_cancel_time | mdf.ghd_late_ind, cancels.cancel_time_utc, mdf.ghd_eta_utc, mdf.dropoff_complete_time_utc | Complex CASE statement: when ghd_late_ind = 1 then 1; when cancel_time_utc > ghd_eta_utc AND dropoff_complete_time_utc IS NULL then 1; else 0 | Question: This business rule significantly expands the definition of "lateness" to include post-ETA cancellations. How does this align with customer experience metrics and SLA definitions? Should cancelled orders be treated equivalently to late deliveries in performance reporting? |
| adjustment_reason_name | adj.adjustment_reason_name, adj.adj_contact_reason, cancels.cancel_contact_reason | Extensive CASE statement with REGEXP_LIKE patterns that standardizes various reason texts into consistent categories: 'food temperature', 'incorrect order', 'food damaged', 'missing item', 'item removed from order', 'late order', 'order or menu issue', 'out of item', 'missed delivery', plus special handling for generic 'refund due to' patterns | Question: This standardization logic contains numerous hardcoded regex patterns (e.g., 'food temp\|cold\|quality_temp\|temperature'). Is there a centralized reference document for all these patterns? How are new reason types or pattern variations handled when they appear in production data? |
| adj_contact_reason | adj.adj_contact_reason | Direct selection from adj CTE | None |
| tip | integrated_order.order_contribution_profit_fact.tip | Direct selection from the source table | Tip amount (may relate to driver satisfaction metrics) |
| bundle_ind | mdf.bundle_ind | Direct selection from mdf CTE | Bundle indicator |
| shop_and_pay_ind | mdf.shop_and_pay_ind | Direct selection from mdf CTE | Shop and Pay indicator |
| fg_reason | ghg.fg_reason, care_fg.fg_reason, integrated_order.order_contribution_profit_fact.cp_care_concession_awarded_amount | Complex CASE statement that only processes when cp_care_concession_awarded_amount ≠ 0, then applies identical REGEXP_LIKE patterns as adjustment_reason_name to standardize fg_reason values, else returns NULL when no concession | Question: The regex standardization patterns are duplicated from the adjustment_reason_name logic. Should these be consolidated into a shared function or reference table to maintain consistency and reduce maintenance overhead? |
| care_cost_reason | csv_sandbox.care_cost_reasons.care_cost_reason | Direct selection joined on contacts.latest_contact_reason = ccr.scr (secondary contact reason) | Question: The join is specifically on secondary contact reason (scr), but earlier CTEs prioritize secondary over primary reasons. Is there a corresponding lookup for primary contact reasons, and what percentage of contacts would be excluded due to this specific join condition? |
| care_cost_group | csv_sandbox.care_cost_reasons.care_cost_group | Uses COALESCE to default to 'not grouped' if null | None |
| cp_care_concession_awarded_amount | integrated_order.order_contribution_profit_fact.cp_care_concession_awarded_amount | Direct selection from the source table | Concession cost component |
| driver_pay_per_order | integrated_order.order_contribution_profit_fact.cp_driver_pay_per_order | Uses COALESCE to default to 0 if null | Driver pay component |
| true_up | integrated_order.order_contribution_profit_fact.cp_driver_true_up | Uses COALESCE to default to 0 if null | Driver true-up adjustments |
| cp_redelivery_cost | integrated_order.order_contribution_profit_fact.cp_redelivery_cost | Uses COALESCE to default to 0 if null | Redelivery cost component |
| cp_grubcash_care_concession_awarded_amount | integrated_order.order_contribution_profit_fact.cp_grubcash_care_concession_awarded_amount | Uses COALESCE to default to 0 if null | GrubCash concession component (new) |
| cp_grub_care_refund | integrated_order.order_contribution_profit_fact.cp_grub_care_refund | Uses COALESCE to default to 0 if null | Grub care refund component |
| rr_refund | rest_refunds.rr_refund_total | Uses COALESCE to default to 0 if null | Restaurant refund component (new) |
| cp_care_ticket_cost | integrated_order.order_contribution_profit_fact (multiple ticket cost fields) | Sum of cp_diner_care_tickets + cp_driver_care_tickets + cp_restaurant_care_tickets + cp_gh_internal_care_tickets | Total care ticket cost |

**Date Filtering Logic:**
- order_contribution_profit_fact filtered by: `cpf.order_date >= CURRENT_DATE - INTERVAL '6' MONTH` AND `DATE(cpf.delivery_time_ct) >= CURRENT_DATE - INTERVAL '6' MONTH` AND `DATE(cpf.delivery_time_ct) < CURRENT_DATE`
- Question: Why filter on both order_date and delivery_time_ct? What scenarios would cause these dates to differ significantly beyond 6 months?

**Geographic Join Details:**
- Joins integrated_geo.order_location on order_uuid
- Joins integrated_geo.blockgroup_dim on blockgroup_geoid with condition `substr(m.blockgroup_geoid,12,1)<>'0'`
- Question: What is the significance of the substring condition on blockgroup_geoid? Does this filter out specific geographic areas or invalid geoids?

*Note: This CTE consolidates order-level data from multiple sources and applies critical business logic. It's significantly more complex than the original query version with additional market dimensions, cost components, and the delivery_ind/automated_ind indicators.*

## CTE: o2
Purpose: Creates a consolidated reason field by prioritizing cancellation reasons over adjustment reasons.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| date1 | o.date1 | Pass-through from o CTE | None |
| ghd_ind | o.ghd_ind | Pass-through from o CTE | None |
| delivery_ind | o.delivery_ind | Pass-through from o CTE | None |
| order_uuid | o.order_uuid | Pass-through from o CTE | None |
| cp_diner_adj | o.cp_diner_adj | Pass-through from o CTE | None |
| order_status_cancel_ind | o.order_status_cancel_ind | Pass-through from o CTE | None |
| automated_ind | o.automated_ind | Pass-through from o CTE | None |
| cancel_fact_ind | o.cancel_fact_ind | Pass-through from o CTE | None |
| cancel_group | o.cancel_group | Pass-through from o CTE | None |
| cancel_reason_name | o.cancel_reason_name | Pass-through from o CTE | None |
| cancel_contact_reason | o.cancel_contact_reason | Pass-through from o CTE | None |
| status_cancelled_reached_ind | o.status_cancelled_reached_ind | Pass-through from o CTE | None |
| modified_cbsa_name | o.modified_cbsa_name | Pass-through from o CTE | None |
| key_cities_cbsa | o.key_cities_cbsa | Pass-through from o CTE | None |
| ghd_delivery_region_name | o.ghd_delivery_region_name | Pass-through from o CTE | None |
| large_order_ind | o.large_order_ind | Pass-through from o CTE | None |
| tri_state_ind | o.tri_state_ind | Pass-through from o CTE | None |
| cancelled_order_ind | o.cancelled_order_ind | Pass-through from o CTE | None |
| cp_revenue | o.cp_revenue | Pass-through from o CTE | None |
| ghd_late_ind | o.ghd_late_ind | Pass-through from o CTE | None |
| ghd_late_ind_incl_cancel_time | o.ghd_late_ind_incl_cancel_time | Pass-through from o CTE | None |
| adjustment_and_cancel_reason_combined | o.cancel_reason_name, o.adjustment_reason_name | CASE statement with specific logic: when cancel_reason_name = 'Not Mapped' then adjustment_reason_name; when cancel_reason_name IS NULL then adjustment_reason_name; else LOWER(cancel_reason_name) | Question: Why is the 'Not Mapped' literal value treated equivalently to NULL? Is 'Not Mapped' a standard placeholder value in the cancellation reason data, and what are the data quality implications of this fallback logic? |
| fg_reason | o.fg_reason | Pass-through from o CTE | None |
| cp_care_concession_awarded_amount | o.cp_care_concession_awarded_amount | Pass-through from o CTE | None |
| driver_pay_per_order | o.driver_pay_per_order | Pass-through from o CTE | None |
| true_up | o.true_up | Pass-through from o CTE | None |
| tip | o.tip | Pass-through from o CTE | None |
| bundle_ind | o.bundle_ind | Pass-through from o CTE | None |
| shop_and_pay_ind | o.shop_and_pay_ind | Pass-through from o CTE | None |
| cp_care_ticket_cost | o.cp_care_ticket_cost | Pass-through from o CTE | None |
| care_cost_reason | o.care_cost_reason | Pass-through from o CTE | None |
| care_cost_group | o.care_cost_group | Pass-through from o CTE | None |
| cp_redelivery_cost | o.cp_redelivery_cost | Pass-through from o CTE | None |
| cp_grubcash_care_concession_awarded_amount | o.cp_grubcash_care_concession_awarded_amount | Pass-through from o CTE | None |
| cp_grub_care_refund | o.cp_grub_care_refund | Pass-through from o CTE | None |
| rr_refund | o.rr_refund | Pass-through from o CTE | None |

*Note: This CTE serves as a pass-through layer with minimal transformation, focusing solely on reason consolidation. All other columns are passed through unchanged, suggesting this step is primarily for simplifying downstream logic. Compared to the original query, this CTE now handles many more fields including the new market dimensions and cost components.*

## CTE: o3
Purpose: Calculates total care cost and derives final analytical reason groups for aggregation.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| date1 | o2.date1 | Pass-through from o2 CTE | None |
| ghd_ind | o2.ghd_ind | Pass-through from o2 CTE | None |
| delivery_ind | o2.delivery_ind | Pass-through from o2 CTE | None |
| order_uuid | o2.order_uuid | Pass-through from o2 CTE | None |
| cp_diner_adj | o2.cp_diner_adj | Pass-through from o2 CTE | None |
| order_status_cancel_ind | o2.order_status_cancel_ind | Pass-through from o2 CTE | None |
| automated_ind | o2.automated_ind | Pass-through from o2 CTE | None |
| cancel_fact_ind | o2.cancel_fact_ind | Pass-through from o2 CTE | None |
| cancel_group | o2.cancel_group | Pass-through from o2 CTE | None |
| cancel_reason_name | o2.cancel_reason_name | Pass-through from o2 CTE | None |
| cancel_contact_reason | o2.cancel_contact_reason | Pass-through from o2 CTE | None |
| ghd_late_ind | o2.ghd_late_ind | Pass-through from o2 CTE | None |
| ghd_late_ind_incl_cancel_time | o2.ghd_late_ind_incl_cancel_time | Pass-through from o2 CTE | None |
| adjustment_group | o2.cancel_group, integrated_ref.cancellation_reason_map.cancel_group, o2.adjustment_and_cancel_reason_combined | CASE statement: when o2.cancel_group IS NOT NULL AND o2.cancel_group != 'Other' then crm.cancel_group; else applies REGEXP_LIKE patterns to categorize into 'Logistics Issues' (missed delivery, late, damaged, food temp), 'Restaurant Issues' (missing items, incorrect orders, quality issues), 'Diner Issues' (diner error, change of plans), with fallback to COALESCE(crm.cancel_group, 'not grouped') | Question: This pattern matching logic is replicated from the o CTE with slight variations. Should there be a standardized function or lookup table for these categorization rules? Also, when does cancel_group equal 'Other' and how should those cases be handled? |
| fg_group | o2.cancel_group, integrated_ref.cancellation_reason_map.cancel_group, o2.fg_reason | Identical CASE logic as adjustment_group but applied to fg_reason instead of adjustment_and_cancel_reason_combined | Question: The exact same categorization logic is duplicated here. This suggests these rules are business-critical - should they be centralized to ensure consistency across all reason categorizations? |
| fg_reason | o2.cp_care_concession_awarded_amount, o2.fg_reason, o2.adjustment_and_cancel_reason_combined | CASE statement: when cp_care_concession_awarded_amount < 0 AND fg_reason IS NULL then adjustment_and_cancel_reason_combined; else fg_reason | Question: What business scenario does a negative concession amount represent? Is this a refund that should be linked to the primary order issue reason, and why only when fg_reason is specifically NULL? |
| cp_care_concession_awarded_amount | o2.cp_care_concession_awarded_amount | Pass-through from o2 CTE | None |
| adjustment_and_cancel_reason_combined | o2.adjustment_and_cancel_reason_combined | Pass-through from o2 CTE | None |
| status_cancelled_reached_ind | o2.status_cancelled_reached_ind | Pass-through from o2 CTE | None |
| modified_cbsa_name | o2.modified_cbsa_name | Pass-through from o2 CTE | None |
| key_cities_cbsa | o2.key_cities_cbsa | Pass-through from o2 CTE | None |
| ghd_delivery_region_name | o2.ghd_delivery_region_name | Pass-through from o2 CTE | None |
| large_order_ind | o2.large_order_ind | Pass-through from o2 CTE | None |
| tri_state_ind | o2.tri_state_ind | Pass-through from o2 CTE | None |
| cancelled_order_ind | o2.cancelled_order_ind | Pass-through from o2 CTE | None |
| cp_revenue | o2.cp_revenue | Pass-through from o2 CTE | None |
| driver_pay_per_order | o2.driver_pay_per_order | Pass-through from o2 CTE | None |
| tip | o2.tip | Pass-through from o2 CTE | None |
| true_up | o2.true_up | Pass-through from o2 CTE | None |
| bundle_ind | o2.bundle_ind | Pass-through from o2 CTE | None |
| shop_and_pay_ind | o2.shop_and_pay_ind | Pass-through from o2 CTE | None |
| cp_care_ticket_cost | o2.cp_care_ticket_cost | Pass-through from o2 CTE | None |
| care_cost_reason | o2.care_cost_reason | Pass-through from o2 CTE | None |
| care_cost_group | o2.care_cost_group | Pass-through from o2 CTE | None |
| cp_redelivery_cost | o2.cp_redelivery_cost | Pass-through from o2 CTE | None |
| cp_grubcash_care_concession_awarded_amount | o2.cp_grubcash_care_concession_awarded_amount | Pass-through from o2 CTE | None |
| cp_grub_care_refund | o2.cp_grub_care_refund | Pass-through from o2 CTE | None |
| rr_refund | o2.rr_refund | Pass-through from o2 CTE | None |

**Join Details:**
- LEFT JOIN with integrated_ref.cancellation_reason_map on LOWER(crm.cancel_reason_name) = LOWER(o2.adjustment_and_cancel_reason_combined)

*Note: This CTE performs the final cost categorization with reason grouping logic. Compared to the original query, this no longer calculates total_care_cost (moved to final SELECT), but includes significantly more pass-through fields for the new market dimensions and cost components. The results from this CTE directly feed into the final aggregation layer.*

## Final SELECT
Purpose: Aggregates order-level data into summarized metrics grouped by key analytical dimensions.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| date1 | o3.date1 | Direct pass-through from o3, used in GROUP BY | Primary date dimension for aggregation |
| ghd_ind | o3.ghd_ind | Direct pass-through from o3, used in GROUP BY | Delivery type dimension (ghd/non-ghd) |
| delivery_ind | o3.delivery_ind | Direct pass-through from o3, used in GROUP BY | Order type dimension (delivery/pickup) |
| cancel_fact_ind | o3.cancel_fact_ind | Direct pass-through from o3, used in GROUP BY | Cancellation fact indicator dimension |
| adjustment_group | o3.adjustment_group | Direct pass-through from o3, used in GROUP BY | Adjustment reason grouping dimension |
| adjustment_and_cancel_reason_combined | o3.adjustment_and_cancel_reason_combined | Direct pass-through from o3, used in GROUP BY | Combined reason dimension |
| ghd_late_ind | o3.ghd_late_ind | Direct pass-through from o3, used in GROUP BY | Basic lateness indicator dimension |
| ghd_late_ind_incl_cancel_time | o3.ghd_late_ind_incl_cancel_time | Direct pass-through from o3, used in GROUP BY | Extended lateness indicator dimension |
| bundle_ind | o3.bundle_ind | Direct pass-through from o3, used in GROUP BY | Bundle indicator dimension |
| shop_and_pay_ind | o3.shop_and_pay_ind | Direct pass-through from o3, used in GROUP BY | Shop and Pay indicator dimension |
| automated_ind | o3.automated_ind | Direct pass-through from o3, used in GROUP BY | Automation deflection indicator dimension |
| modified_cbsa_name | o3.modified_cbsa_name | Direct pass-through from o3, used in GROUP BY | CBSA market dimension |
| key_cities_cbsa | o3.key_cities_cbsa | Direct pass-through from o3, used in GROUP BY | Key cities categorization dimension |
| ghd_delivery_region_name | o3.ghd_delivery_region_name | Direct pass-through from o3, used in GROUP BY | Delivery region dimension |
| large_order_ind | o3.large_order_ind | Direct pass-through from o3, used in GROUP BY | Order size dimension |
| tri_state_ind | o3.tri_state_ind | Direct pass-through from o3, used in GROUP BY | Tri-state market dimension |
| care_cost_reason_group | o3 (multiple fields) | LOWER(CASE) statement with hierarchical logic: when total costs = 0 then 'orders with no care cost'; when cp_redelivery_cost < 0 then 'logistics issues'; when adjustment_group != 'not grouped' then adjustment_group; when fg_group != 'not grouped' then fg_group; when care_cost_group != 'not grouped' then care_cost_group; else 'not grouped' | Question: This hierarchy seems important for business attribution. Is this priority order documented in business requirements? Why does redelivery cost override all other groupings when negative? [Interpretation] This creates a waterfall logic where redeliveries are always attributed to logistics, then adjustment reasons, then fg reasons, then contact reasons |
| care_cost_reason | o3.adjustment_and_cancel_reason_combined, o3.fg_reason, o3.care_cost_reason, o3.cp_redelivery_cost | CASE statement: when cp_redelivery_cost < 0 then 'Missed Delivery'; else LOWER(COALESCE(adjustment_and_cancel_reason_combined, fg_reason, care_cost_reason)) | Similar hierarchical logic with redelivery taking precedence |
| orders | o3.order_uuid | COUNT(DISTINCT order_uuid) | Total distinct orders in the group |
| cancels | o3.cancel_fact_ind | SUM(IF(cancel_fact_ind=true, 1, 0)) | Count of cancelled orders |
| missed_revenue_cp | o3.status_cancelled_reached_ind, o3.cancelled_order_ind, o3.cp_revenue | SUM(CASE WHEN status_cancelled_reached_ind = TRUE AND cancelled_order_ind = FALSE THEN cp_revenue END) | Question: What business scenario does this represent - orders that reached cancelled status but are not marked as cancelled in the order contribution profit fact? What revenue is being captured here? |
| cp_diner_adj | o3.cp_diner_adj | SUM(cp_diner_adj) | Total diner adjustments |
| cp_care_concession_awarded_amount | o3.cp_care_concession_awarded_amount | SUM(cp_care_concession_awarded_amount) | Total care concessions |
| cp_care_ticket_cost | o3.cp_care_ticket_cost | SUM(cp_care_ticket_cost) | Total care ticket costs |
| redelivery_cost | o3.cp_redelivery_cost | SUM(cp_redelivery_cost) | Total redelivery costs |
| cp_grubcash_care_concession_awarded_amount | o3.cp_grubcash_care_concession_awarded_amount | SUM(cp_grubcash_care_concession_awarded_amount) | Total GrubCash concessions |
| cp_grubcare_refund | o3.cp_grub_care_refund | SUM(cp_grub_care_refund) | Total Grub care refunds |
| rr_refund | o3.rr_refund | SUM(rr_refund) | Total restaurant refunds |
| cp_total_care_cost | o3 (multiple cost fields) | SUM(cp_diner_adj + cp_care_concession_awarded_amount + cp_care_ticket_cost + cp_redelivery_cost + cp_grub_care_refund) | **Primary business metric**: total fulfillment care cost aggregated from all cost components. Question: This excludes cp_grubcash_care_concession_awarded_amount and rr_refund from the total. Why are these tracked separately but not included in total care cost? |
| ghd_late_count | o3.ghd_late_ind | SUM(IF(ghd_late_ind=1, 1, 0)) | Count of late deliveries |
| ghd_orders | o3.ghd_ind | SUM(IF(ghd_ind='ghd', 1, 0)) | Count of GHD orders |
| driver_pay_cpf | o3.driver_pay_per_order | SUM(driver_pay_per_order) | Total driver pay from order contribution profit fact |
| tip | o3.tip | SUM(tip) | Total tips |
| true_up | o3.true_up | SUM(true_up) | Total driver true-up adjustments |
| orders_with_adjustments | o3.cp_diner_adj | SUM(IF(cp_diner_adj<0, 1, 0)) | Count of orders with diner adjustments |
| orders_with_fg | o3.cp_care_concession_awarded_amount | SUM(IF(cp_care_concession_awarded_amount<0, 1, 0)) | Count of orders with concessions |
| orders_with_redelivery | o3.cp_redelivery_cost | SUM(IF(cp_redelivery_cost<0, 1, 0)) | Count of orders with redeliveries |
| orders_with_gh_credit | o3.cp_grubcash_care_concession_awarded_amount | SUM(IF(cp_grubcash_care_concession_awarded_amount<0, 1, 0)) | Count of orders with GrubCash credits |
| orders_with_gh_credit_refund | o3.cp_grub_care_refund | SUM(IF(cp_grub_care_refund<0, 1, 0)) | Count of orders with Grub care refunds |
| orders_with_rr_refund | o3.rr_refund | SUM(IF(ABS(rr_refund)>0, 1, 0)) | Count of orders with restaurant refunds |
| orders_with_care_cost | o3 (multiple cost fields) | SUM(CASE WHEN (cp_diner_adj + cp_care_concession_awarded_amount + cp_care_ticket_cost + cp_redelivery_cost + cp_grub_care_refund) < 0 THEN 1 ELSE 0 END) | Count of orders with any care cost. Question: This metric uses the same cost components as cp_total_care_cost. Why measure both the total amount and order count using the same subset of costs? |
| cancels_osmf_definition | o3.order_status_cancel_ind, o3.order_uuid | COUNT(CASE WHEN order_status_cancel_ind = true THEN order_uuid END) | Question: The "osmf_definition" suffix is unclear - does this refer to order status milestone fact methodology? How does this cancellation count differ from the 'cancels' metric above, and when should each be used? |

**GROUP BY Dimensions:**
The query groups by 16 dimensions: date1, ghd_ind, delivery_ind, cancel_fact_ind, adjustment_group, adjustment_and_cancel_reason_combined, ghd_late_ind, ghd_late_ind_incl_cancel_time, bundle_ind, shop_and_pay_ind, automated_ind, modified_cbsa_name, key_cities_cbsa, ghd_delivery_region_name, large_order_ind, tri_state_ind

*Note: Compared to the original query, the LATEST version has significantly more grouping dimensions (16 vs ~3) and more granular cost breakdowns. The care_cost_reason_group logic is moved here from o3 CTE, and total_care_cost calculation is also performed in the final SELECT rather than in o3.*

## Summary of Key Findings and Recommendations

**Critical Differences from Original Query:**
1. **Date Window Approach:** Rolling 6-month window vs parameterized date range - impacts historical analysis and reporting consistency
2. **New Business Dimensions:** CBSA markets, delivery regions, tri-state segmentation, large order categorization
3. **New Business Indicators:** Shop and Pay orders, automation deflection, delivery vs pickup
4. **Additional Cost Components:** GrubCash concessions, restaurant refunds, driver true-up
5. **Different Aggregation Strategy:** MIN_BY in mdf CTE suggests different approach to handling duplicate delivery records
6. **Expanded Metrics:** More granular cost breakdowns and order counts by cost type

**Immediate Business Attention Required:**
1. **Rolling Date Window Impact:** 6-month rolling window means query results change daily as oldest data drops off. Historical trend analysis and period-over-period comparisons require careful date range management
2. **Reason Standardization Maintenance:** The extensive regex pattern logic in CTE o requires centralized management to prevent inconsistencies as new reason types emerge
3. **Market Segmentation Complexity:** Multiple overlapping geographic dimensions (CBSA, regions, tri-state) need clear business definitions to avoid confusion in reporting
4. **Data Quality Monitoring:** Multiple MAX_BY/MIN_BY functions suggest potential duplicate records in source systems requiring investigation
5. **Cost Component Alignment:** Clarification needed on why GrubCash concessions and restaurant refunds are tracked but excluded from total care cost

**Business Logic Clarifications Needed:**
1. **Secondary vs Primary Contact Reason Prioritization:** Consistent pattern across CTEs but business rationale unclear
2. **ETA-Related Metrics Definition:** Expanded lateness definition including post-ETA cancellations needs SLA alignment review
3. **Cost Component Variations:** Different cost component combinations used in various metrics need business justification
4. **Multiple Cancellation Indicators:** order_status_cancel_ind, cancel_fact_ind, status_cancelled_reached_ind, cancelled_order_ind serve different purposes - clear documentation needed
5. **Delivery vs Pickup Care Costs:** How pickup orders generate fulfillment care costs and whether cost patterns differ significantly
6. **Shop and Pay Impact:** Whether Shop and Pay orders have different care cost profiles requiring separate analysis
7. **Automation Deflection:** How automation deflection relates to care costs and ticket cost calculations
8. **Tri-State Definition:** Why Tri-State is tracked separately from other geographic segments and how it's used in analysis
9. **Large Order Thresholds:** Whether $250 and $1000 thresholds align with business definitions and operational differences

**Technical Optimization Opportunities:**
1. **Performance:** Complex regex matching and multiple joins may benefit from indexing analysis, especially with 6-month rolling window
2. **Maintainability:** Lookup tables could replace CASE statement logic for reason categorization
3. **Consistency:** Duplicate logic patterns should be consolidated into shared functions or reference tables
4. **Date Filter Efficiency:** Multiple date filters across CTEs with 6-month window - consider materialized views for frequently accessed time periods
5. **Aggregation Pattern:** MIN_BY usage in mdf CTE should be documented and validated for correctness

**Data Completeness Considerations:**
- Extensive COALESCE and fallback logic indicates significant missing data patterns
- Impact of NULL handling choices on business metrics needs validation
- Cross-CTE data flow dependencies require monitoring for data quality issues
- Rolling 6-month window requires monitoring for data latency and completeness at window boundaries

**Reporting and Analytics Implications:**
1. **Historical Analysis:** Rolling window complicates year-over-year comparisons - consider snapshot approach for historical reporting
2. **Market Segmentation:** Multiple geographic dimensions enable detailed market analysis but require clear reporting hierarchies
3. **Cost Attribution:** Hierarchical logic in care_cost_reason_group ensures clear attribution but may mask underlying issues
4. **Operational Metrics:** New indicators (automation, shop and pay) support operational improvement initiatives
5. **Revenue Impact:** missed_revenue_cp metric provides visibility into cancellation revenue impact
