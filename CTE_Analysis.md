# CTE Analysis - Fulfillment Care Cost Query

This document provides a detailed, column-level logical analysis of each Common Table Expression (CTE) in the fulfillment_care_cost.sql query. The analysis is based solely on the SQL query and the context provided in "Breaking Down Logistic Care Costs Query.md".

## Analysis Overview
The query processes order fulfillment care costs through 11 CTEs that progressively build upon each other. The main data flow is: source data extraction → reason standardization → data integration → cost calculation → final aggregation. Key business logic includes extensive regex-based reason categorization and hierarchical cost attribution.

**Critical Data Quality Patterns Identified:**
- Extensive use of MAX_BY functions suggests potential duplicate records in source tables
- Multiple COALESCE and fallback logic patterns indicate missing data handling is a significant concern
- Regex pattern duplication across CTEs creates maintenance risks
- Hierarchical reason attribution may mask underlying data quality issues

**Performance and Complexity Considerations:**
- Complex regex matching in o CTE applied to large datasets may impact performance
- Multiple LEFT JOINs in primary integration CTE (o) could benefit from indexing analysis
- Hardcoded UUID lists in market segmentation create maintenance overhead
- Extensive CASE statement logic suggests potential for lookup table optimization

## CTE: adj
Purpose: Identifies orders with Grubhub-paid refunds and retrieves the latest adjustment reason and associated contact reason for each order.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| order_uuid | source_cass_rainbow_data.adjustment_reporting.order_uuid | Direct selection from the source table | None |
| adjustment_reason_name | source_cass_rainbow_data.adjustment_reporting.reason | Uses MAX_BY function to get the reason corresponding to the latest adjustment_timestamp_utc for each order | Question: The use of MAX_BY suggests multiple adjustment records per order are possible. What is the frequency of multiple adjustments per order, and could this aggregation be masking important business insights about repeated adjustment patterns? |
| adj_contact_reason | source_zendesk_ref.secondary_contact_reason.name, source_zendesk_ref.primary_contact_reason.name | Uses MAX_BY with COALESCE(sr.name, pr.name) to get the contact reason corresponding to the latest adjustment_timestamp_utc, prioritizing secondary over primary | Question: What is the business rationale for prioritizing secondary contact reason over primary contact reason? Is this hierarchy documented in business requirements? |

## CTE: ghg  
Purpose: Identifies orders with granted Grubhub Guarantee claims and categorizes them into standardized reason types.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| order_uuid | ods.carecontactfacade_guarantee_claim.cart_uuid | Direct selection from source, with alias cart_uuid mapped to order_uuid | Question: Is the cart_uuid to order_uuid mapping always 1:1? Are there any scenarios where this relationship might differ? |
| fg_reason | ods.carecontactfacade_guarantee_claim.claim_type | CASE statement with MAX aggregation that maps 'SERVICE' → 'Late Delivery - GHG' and 'PRICING' → 'Price - GHG' | Question: What happens to guarantee claims with claim_type values other than 'SERVICE' or 'PRICING'? Are there other claim types in the data that should be handled? |

## CTE: care_fg
Purpose: Identifies orders that had concessions and retrieves the latest contact reason associated with those concessions.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| order_uuid | source_cass_rainbow_data.concession_reporting.order_uuid | Direct selection from the source table | None |
| fg_reason | source_zendesk_ref.secondary_contact_reason.name, source_zendesk_ref.primary_contact_reason.name | Uses MAX_BY with COALESCE to get the contact reason (secondary preferred over primary) corresponding to the latest issue_timestamp_utc | Question: Similar to adj CTE, why is secondary contact reason preferred? Is this a consistent business rule across all contact reason retrievals? |

## CTE: diner_ss_cancels
Purpose: Identifies orders with diner self-service cancellations and maps reason codes to standardized descriptions and groups.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| order_uuid | ods.carereporting_cancellation_result.order_id | Direct selection with alias order_id mapped to order_uuid | None |
| reason_code | ods.carereporting_cancellation_result.reason_code | Uses MAX aggregation to select a representative reason_code per order | Question: Why use MAX aggregation instead of latest by timestamp or most frequent reason? If multiple reason codes exist per order, what is the business justification for this selection method? |
| diner_ss_cancel_reason | ods.carereporting_cancellation_result.reason_code | CASE statement mapping: 'DINER_PAYMENT_ISSUE' → 'Payment Issues', 'DINER_CHOSE_WRONG_ADDRESS' → 'Delivery Information Incorrect', 'DINER_CHOSE_WRONG_ORDER_ITEMS' → 'Ordered By Mistake', 'DINER_DOES_NOT_WANT_LATE_ORDER' → 'Late Order', 'DINER_DOES_NOT_WANT_THE_FOOD' → 'Change of Plans' | Question: Is this the complete set of possible reason_code values in the source data? What happens to unmapped reason codes - are they set to NULL or excluded? |
| diner_ss_cancel_reason_group | ods.carereporting_cancellation_result.reason_code | CASE statement mapping reason codes to two groups: 'Diner Issues' (payment, wrong address, wrong items, change of plans) and 'Logistics Issues' (late order only) | Question: The mapping classifies most cancellations as 'Diner Issues' except lateness. How does this align with the overall fulfillment cost attribution framework used elsewhere in the query? |

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

## CTE: mdf
Purpose: Retrieves comprehensive delivery and operational data for Grubhub-managed deliveries with derived analytical fields.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| order_uuid | integrated_delivery.managed_delivery_fact_v2.order_uuid | Direct selection from the source table | None |
| region_uuid | integrated_delivery.managed_delivery_fact_v2.region_uuid | Direct selection from the source table | None |
| region_name | integrated_delivery.managed_delivery_fact_v2.region_name | Direct selection from the source table | None |
| mealtime | integrated_delivery.managed_delivery_fact_v2.mealtime | Direct selection from the source table | None |
| delivery_eta_type | integrated_delivery.managed_delivery_fact_v2.delivery_eta_type | Direct selection from the source table | Question: What are the possible values for delivery_eta_type and what do they represent? |
| CA_Market | integrated_delivery.managed_delivery_fact_v2.region_name | CASE statement that returns 'CA' if region_name starts with 'CA%', otherwise 'xCA' | Question: Is the 'CA%' pattern reliable for identifying California markets? Are there any edge cases? |
| NYC_Market | integrated_delivery.managed_delivery_fact_v2.region_uuid | CASE statement with hardcoded list of 17 specific region UUIDs that map to 'DCWP', otherwise 'xDCWP' | Question: This hardcoded UUID list creates maintenance overhead and potential data inconsistency. Is there a reference table or more maintainable approach for identifying DCWP markets? Additionally, what is the business impact if new regions are added to this market segment but the query isn't updated? |
| bundle_ind | integrated_delivery.managed_delivery_fact_v2.bundle_type | Boolean logic checking if bundle_type IS NOT NULL | Question: What constitutes a bundle and what are the possible bundle_type values? |
| future | integrated_delivery.managed_delivery_fact_v2.future_order_ind | Boolean conversion with explicit TRUE/FALSE casting | None |
| start_of_week | integrated_delivery.managed_delivery_fact_v2.dropoff_complete_time_local, eta_at_order_placement_time_local, order_created_time_local | Complex DATE and DATE_TRUNC logic using COALESCE with fallback hierarchy | Question: Why this specific fallback hierarchy? What's the business significance of using eta_at_order_placement_time_local as middle priority? |
| date2 | integrated_delivery.managed_delivery_fact_v2.dropoff_complete_time_local, eta_at_order_placement_time_local, order_created_time_local | DATE function applied to the same COALESCE hierarchy as start_of_week | [Interpretation] This appears to be the primary analytical date for the order |
| week | integrated_delivery.managed_delivery_fact_v2.dropoff_complete_time_local, eta_at_order_placement_time_local, order_created_time_local | WEEK function applied to the COALESCE hierarchy | None |
| month | integrated_delivery.managed_delivery_fact_v2.dropoff_complete_time_local, eta_at_order_placement_time_local, order_created_time_local | MONTH function applied to the COALESCE hierarchy | None |
| deliverytime_utc | integrated_delivery.managed_delivery_fact_v2.dropoff_complete_time_utc, eta_at_order_placement_time_utc, order_created_time_utc | COALESCE hierarchy similar to date fields but in UTC | None |
| dayofweek | integrated_delivery.managed_delivery_fact_v2.dropoff_complete_time_local, eta_at_order_placement_time_local, order_created_time_local | FORMAT_DATETIME with 'E' format applied to COALESCE hierarchy | None |
| datetime_local | integrated_delivery.managed_delivery_fact_v2.dropoff_complete_time_local, eta_at_order_placement_time_local, order_created_time_local | Direct COALESCE hierarchy without additional transformation | None |
| diner_ty_eta | integrated_delivery.managed_delivery_fact_v2 (multiple timestamp fields) | Complex calculation: DATE_DIFF('second', IF(future_order_ind IS NULL OR FALSE, order_created_time_utc, delivery_created_time_utc), lower_bound_eta_at_order_placement_time_utc) / 60.0 - calculates time difference in minutes between order/delivery creation and ETA lower bound | Question: The acronym "diner_ty_eta" is unclear - does "ty" stand for "thank you" or something else? More importantly, this calculation has different behavior for future vs immediate orders - are both use cases documented, and what are the business implications of using different baseline timestamps? |
| dropoff_complete_time_utc | integrated_delivery.managed_delivery_fact_v2.dropoff_complete_time_utc | Direct selection from the source table | None |
| ghd_eta_utc | integrated_delivery.managed_delivery_fact_v2.eta_at_order_placement_time_utc | Adds 10 minutes to eta_at_order_placement_time_utc | Question: Why exactly 10 minutes? Is this a standard buffer time for GHD deliveries? |
| ghd_late_ind | integrated_delivery.managed_delivery_fact_v2.eta_at_order_placement_time_utc, dropoff_complete_time_utc | CASE statement checking if actual delivery time exceeds estimated time (plus 10 minute buffer) | [Interpretation] This is the primary lateness indicator for GHD orders |
| cancel_ind | integrated_delivery.managed_delivery_fact_v2.cancelled_time_local | Boolean check if cancelled_time_local IS NOT NULL | None |
| cancel_mins | integrated_delivery.managed_delivery_fact_v2.cancelled_time_local, click_start_time_local | DATE_DIFF in minutes between click_start_time_local and cancelled_time_local, only when cancelled | Question: What does click_start_time_local represent? Is this when the customer started the ordering process? |

## CTE: contacts
Purpose: Identifies orders with worked care contacts and retrieves the latest contact information.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| order_uuid | integrated_core.ticket_fact.order_uuid | Direct selection from the source table | None |
| latest_ticket_id | integrated_core.ticket_fact.ticket_id | Uses MAX_BY to get ticket_id corresponding to the latest created_time | None |
| latest_contact_reason | source_zendesk_ref.secondary_contact_reason.name, source_zendesk_ref.primary_contact_reason.name | Uses MAX_BY with COALESCE to get contact reason corresponding to latest created_time | Consistent with pattern in other CTEs |
| contacts | integrated_core.ticket_fact.ticket_id | COUNT of ticket_id records | Represents total number of care contacts for the order |

## CTE: o
Purpose: Integrates operational and financial data, standardizes issue reasons, and calculates specific costs for comprehensive order analysis.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| date1 | integrated_order.order_contribution_profit_fact.delivery_time_ct | DATE function applied to delivery_time_ct | None |
| region_uuid | mdf.region_uuid | Direct selection from mdf CTE | None |
| region_name | mdf.region_name | Direct selection from mdf CTE | None |
| CA_Market | mdf.CA_Market | Direct selection from mdf CTE | None |
| NYC_Market | mdf.NYC_Market | Direct selection from mdf CTE | None |
| diner_ty_eta | mdf.diner_ty_eta | Direct selection from mdf CTE | None |
| delivery_eta_type | mdf.delivery_eta_type | Direct selection from mdf CTE | None |
| mealtime | mdf.mealtime | Direct selection from mdf CTE | None |
| month | mdf.month | Direct selection from mdf CTE | None |
| start_of_week | mdf.start_of_week | Direct selection from mdf CTE | None |
| week | mdf.week | Direct selection from mdf CTE | None |
| date2 | mdf.date2 | Direct selection from mdf CTE | None |
| deliverytime_utc | mdf.deliverytime_utc | Direct selection from mdf CTE | None |
| dayofweek | mdf.dayofweek | Direct selection from mdf CTE | None |
| datetime_local | mdf.datetime_local | Direct selection from mdf CTE | None |
| time_local | mdf.datetime_local | CAST with CONCAT to extract HOUR and MINUTE as TIME format | [Interpretation] Converts datetime to time-only format for analytical purposes |
| ghd_ind | integrated_order.order_contribution_profit_fact.managed_delivery_ind | IF statement converting boolean to 'ghd'/'non-ghd' text values | None |
| order_uuid | integrated_order.order_contribution_profit_fact.order_uuid | Direct selection from the source table | None |
| cp_diner_adj | integrated_order.order_contribution_profit_fact.cp_diner_adj | Direct selection from the source table | None |
| cancel_ind | mdf.cancel_ind | Direct selection from mdf CTE | None |
| cancel_mins | mdf.cancel_mins | Direct selection from mdf CTE | None |
| order_status_cancel_ind | cancels.order_status_cancel_ind | Uses COALESCE to default to FALSE if null | None |
| cancel_fact_ind | cancels.order_uuid | IF statement checking if cancels.order_uuid IS NOT NULL | Boolean indicator of whether order appears in cancellation fact |
| cancel_group | cancels.cancel_group | Direct selection from cancels CTE | None |
| cancel_reason_name | cancels.cancel_reason_name | Direct selection from cancels CTE | None |
| cancel_contact_reason | cancels.cancel_contact_reason | Direct selection from cancels CTE | None |
| ghd_late_ind | mdf.ghd_late_ind | Uses COALESCE to default to 0 if null | None |
| ghd_late_ind_incl_cancel_time | mdf.ghd_late_ind, cancels.cancel_time_utc, mdf.ghd_eta_utc, mdf.dropoff_complete_time_utc | Complex CASE statement: when ghd_late_ind = 1 then 1; when cancel_time_utc > ghd_eta_utc AND dropoff_complete_time_utc IS NULL then 1; else 0 - extends lateness definition to include cancellations that occur after ETA when no delivery completion exists | Question: This business rule significantly expands the definition of "lateness" to include post-ETA cancellations. How does this align with customer experience metrics and SLA definitions? Should cancelled orders be treated equivalently to late deliveries in performance reporting? |
| adjustment_reason_name | adj.adjustment_reason_name, adj.adj_contact_reason, cancels.cancel_contact_reason | Extensive CASE statement with REGEXP_LIKE patterns that standardizes various reason texts into consistent categories: 'food temperature', 'incorrect order', 'food damaged', 'missing item', 'item removed from order', 'late order', 'order or menu issue', 'out of item', 'missed delivery', plus special handling for generic 'refund due to' patterns | Question: This standardization logic contains numerous hardcoded regex patterns (e.g., 'food temp\|cold\|quality_temp\|temperature'). Is there a centralized reference document for all these patterns? How are new reason types or pattern variations handled when they appear in production data? |
| adj_contact_reason | adj.adj_contact_reason | Direct selection from adj CTE | None |
| driver_pay_per_order | integrated_order.order_contribution_profit_fact.driver_pay_per_order | Direct selection from the source table | None |
| tip | integrated_order.order_contribution_profit_fact.tip | Direct selection from the source table | None |
| bundle_ind | mdf.bundle_ind | Direct selection from mdf CTE | None |
| fg_reason | ghg.fg_reason, care_fg.fg_reason, integrated_order.order_contribution_profit_fact.cp_care_concession_awarded_amount | Complex CASE statement that only processes when cp_care_concession_awarded_amount ≠ 0, then applies identical REGEXP_LIKE patterns as adjustment_reason_name to standardize fg_reason values | Question: The regex standardization patterns are duplicated from the adjustment_reason_name logic. Should these be consolidated into a shared function or reference table to maintain consistency and reduce maintenance overhead? |
| care_cost_reason | csv_sandbox.care_cost_reasons.care_cost_reason | Direct selection joined on contacts.latest_contact_reason = ccr.scr (secondary contact reason) | Question: The join is specifically on secondary contact reason (scr), but earlier CTEs prioritize secondary over primary reasons. Is there a corresponding lookup for primary contact reasons, and what percentage of contacts would be excluded due to this specific join condition? |
| care_cost_group | csv_sandbox.care_cost_reasons.care_cost_group | Uses COALESCE to default to 'not grouped' if null | None |
| cp_care_concession_awarded_amount | integrated_order.order_contribution_profit_fact.cp_care_concession_awarded_amount | Direct selection from the source table | None |
| cp_grub_care_refund | integrated_order.order_contribution_profit_fact.cp_grub_care_refund | Direct selection from the source table | None |
| cp_redelivery_cost | integrated_order.order_contribution_profit_fact.cp_redelivery_cost | Direct selection from the source table | None |
| cp_care_ticket_cost | integrated_order.order_contribution_profit_fact (multiple ticket cost fields) | Sum of cp_diner_care_tickets + cp_driver_care_tickets + cp_restaurant_care_tickets + cp_gh_internal_care_tickets | None |

## CTE: o2
Purpose: Creates a consolidated reason field by prioritizing cancellation reasons over adjustment reasons.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| adjustment_and_cancel_reason_combined | o.cancel_reason_name, o.adjustment_reason_name | CASE statement with specific logic: when cancel_reason_name = 'Not Mapped' then adjustment_reason_name; when cancel_reason_name IS NULL then adjustment_reason_name; else LOWER(cancel_reason_name) | Question: Why is the 'Not Mapped' literal value treated equivalently to NULL? Is 'Not Mapped' a standard placeholder value in the cancellation reason data, and what are the data quality implications of this fallback logic? |

*Note: This CTE serves as a pass-through layer with minimal transformation, focusing solely on reason consolidation. All other columns (dates, costs, indicators, etc.) are passed through unchanged, suggesting this step is primarily for simplifying downstream logic.*

## CTE: o3
Purpose: Calculates total care cost and derives final analytical reason groups for aggregation.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| total_care_cost | o2 (multiple cost fields) | Sum of cp_care_concession_awarded_amount + cp_care_ticket_cost + cp_diner_adj + IF(cp_redelivery_cost IS NULL, 0, cp_redelivery_cost) + IF(cp_grub_care_refund IS NULL, 0.00, cp_grub_care_refund) | This is the primary metric calculated by the query |
| cp_redelivery_cost | o2.cp_redelivery_cost | IF statement converting null to 0 | Ensures null values don't break the total_care_cost calculation |
| adjustment_group | o2.cancel_group, integrated_ref.cancellation_reason_map.cancel_group, o2.adjustment_and_cancel_reason_combined | CASE statement: when o2.cancel_group IS NOT NULL AND ≠ 'Other' then crm.cancel_group; else applies REGEXP_LIKE patterns to categorize into 'Logistics Issues' (missed delivery, late, damaged, food temp), 'Restaurant Issues' (missing items, incorrect orders, quality issues), 'Diner Issues' (diner error, change of plans), with fallback to COALESCE(crm.cancel_group, 'not grouped') | Question: This pattern matching logic is replicated from the o CTE with slight variations. Should there be a standardized function or lookup table for these categorization rules? Also, when does cancel_group equal 'Other' and how should those cases be handled? |
| fg_group | o2.cancel_group, integrated_ref.cancellation_reason_map.cancel_group, o2.fg_reason | Identical CASE logic as adjustment_group but applied to fg_reason instead of adjustment_and_cancel_reason_combined | Question: The exact same categorization logic is duplicated here. This suggests these rules are business-critical - should they be centralized to ensure consistency across all reason categorizations? |
| fg_reason | o2.cp_care_concession_awarded_amount, o2.fg_reason, o2.adjustment_and_cancel_reason_combined | CASE statement: when cp_care_concession_awarded_amount < 0 AND fg_reason IS NULL then adjustment_and_cancel_reason_combined; else fg_reason | Question: What business scenario does a negative concession amount represent? Is this a refund that should be linked to the primary order issue reason, and why only when fg_reason is specifically NULL? |

*Note: Most other columns from the o2 CTE are passed through directly*

## Final SELECT
Purpose: Aggregates order-level data into summarized metrics grouped by key analytical dimensions.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| cany_ind | o3.CA_Market, o3.NYC_Market | CASE statement that prioritizes CA_Market='CA', then NYC_Market='DCWP', otherwise 'ROM' | [Interpretation] Creates market segments: California, DC/Washington/Philadelphia, Rest of Markets |
| care_cost_reason_group | o3.total_care_cost, o3.adjustment_group, o3.fg_group, o3.care_cost_group | Hierarchical CASE statement starting with zero cost check, then adjustment_group, fg_group, and finally care_cost_group | Question: This hierarchy seems important for business attribution. Is this priority order documented in business requirements? |
| eta_care_reasons | o3.adjustment_and_cancel_reason_combined, o3.fg_reason, o3.care_cost_reason | CASE statement using IN clause to check for specific ETA-related values: 'order eta update', 'delivery estimate confirmation', 'diner requested cancel - order too late', 'late delivery', 'late order' (with some duplicate entries in the list) | Question: The hardcoded list contains duplicate values ('delivery estimate confirmation' appears 3 times). Is this intentional for emphasis or should it be cleaned up? Also, what is the business definition that distinguishes ETA issues from other logistics issues? |
| orders | o3.order_uuid | COUNT of all order records | Standard row count |
| distinct_order_uuid | o3.order_uuid | COUNT(DISTINCT order_uuid) | Check for potential duplication in the dataset |
| total_care_cost | o3.total_care_cost | SUM of total_care_cost | Primary aggregated metric |
| ghd_orders | o3.ghd_ind | SUM with IF condition counting records where ghd_ind = 'ghd' | Count of Grubhub-delivered orders |
| orders_with_care_cost | o3.cp_diner_adj, o3.cp_care_concession_awarded_amount, o3.cp_care_ticket_cost | SUM with IF condition counting records where (cp_diner_adj + cp_care_concession_awarded_amount + cp_care_ticket_cost) < 0 | Question: This metric excludes cp_redelivery_cost and cp_grub_care_refund from the total_care_cost calculation used elsewhere in the query. What is the business rationale for using a subset of cost components here? Are redelivery and refund costs not considered "care costs" for this particular metric? |
| cancels_osmf_definition | o3.order_status_cancel_ind, o3.order_uuid | COUNT with CASE condition counting records where order_status_cancel_ind = TRUE | Question: The "osmf_definition" suffix is unclear - does this refer to a specific system or methodology? How does this cancellation count differ from other cancellation indicators used throughout the query (cancel_ind, cancel_fact_ind), and when should each be used? |