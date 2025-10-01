# CTE Analysis - Fulfillment Care Cost Query

This document provides a detailed, column-level logical analysis of each Common Table Expression (CTE) in the fulfillment_care_cost.sql query. The analysis is based solely on the SQL query and the context provided in "Breaking Down Logistic Care Costs Query.md".

## CTE: adj
Purpose: Identifies orders with Grubhub-paid refunds and retrieves the latest adjustment reason and associated contact reason for each order.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| order_uuid | source_cass_rainbow_data.adjustment_reporting.order_uuid | Direct selection from the source table | None |
| adjustment_reason_name | source_cass_rainbow_data.adjustment_reporting.reason | Uses MAX_BY function to get the reason corresponding to the latest adjustment_timestamp_utc for each order | Question: The context mentions this is the "formally recorded cause" but doesn't specify what format or standardization this field follows. Can the business logic for reason categorization be clarified? |
| adj_contact_reason | source_zendesk_ref.secondary_contact_reason.name, source_zendesk_ref.primary_contact_reason.name | Uses MAX_BY with COALESCE to get the contact reason (secondary preferred over primary) corresponding to the latest adjustment_timestamp_utc | Question: Why is secondary contact reason preferred over primary contact reason? What is the business logic behind this prioritization? |

## CTE: ghg
Purpose: Identifies orders with granted Grubhub Guarantee claims and categorizes them into standardized reason types.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| order_uuid | ods.carecontactfacade_guarantee_claim.cart_uuid | Direct selection from source, with alias cart_uuid mapped to order_uuid | None |
| fg_reason | ods.carecontactfacade_guarantee_claim.claim_type | CASE statement that maps 'SERVICE' to 'Late Delivery - GHG' and 'PRICING' to 'Price - GHG', using MAX aggregation | Question: Are there other claim_type values that should be handled? What happens to claims with different claim_type values? |

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
| reason_code | ods.carereporting_cancellation_result.reason_code | Uses MAX aggregation to select a representative reason_code per order | Question: Why use MAX instead of the latest by timestamp? Could there be multiple reason codes per order and what's the business logic for selecting one? |
| diner_ss_cancel_reason | ods.carereporting_cancellation_result.reason_code | CASE statement mapping specific reason codes to descriptive text (e.g., 'DINER_PAYMENT_ISSUE' to 'Payment Issues') | Question: Are these all the possible reason_code values? What happens to unmapped reason codes? |
| diner_ss_cancel_reason_group | ods.carereporting_cancellation_result.reason_code | CASE statement mapping reason codes to broader groups ('Diner Issues' or 'Logistics Issues') | Question: The mapping shows most issues as 'Diner Issues' except late orders. Is this categorization aligned with the overall fulfillment cost attribution logic? |

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
| NYC_Market | integrated_delivery.managed_delivery_fact_v2.region_uuid | CASE statement with hardcoded list of 17 specific region UUIDs that map to 'DCWP', otherwise 'xDCWP' | Question: This hardcoded list seems brittle. Is there a more maintainable way to identify DCWP markets? What do these UUIDs specifically represent? |
| bundle_ind | integrated_delivery.managed_delivery_fact_v2.bundle_type | Boolean logic checking if bundle_type IS NOT NULL | Question: What constitutes a bundle and what are the possible bundle_type values? |
| future | integrated_delivery.managed_delivery_fact_v2.future_order_ind | Boolean conversion with explicit TRUE/FALSE casting | None |
| start_of_week | integrated_delivery.managed_delivery_fact_v2.dropoff_complete_time_local, eta_at_order_placement_time_local, order_created_time_local | Complex DATE and DATE_TRUNC logic using COALESCE with fallback hierarchy | Question: Why this specific fallback hierarchy? What's the business significance of using eta_at_order_placement_time_local as middle priority? |
| date2 | integrated_delivery.managed_delivery_fact_v2.dropoff_complete_time_local, eta_at_order_placement_time_local, order_created_time_local | DATE function applied to the same COALESCE hierarchy as start_of_week | [Interpretation] This appears to be the primary analytical date for the order |
| week | integrated_delivery.managed_delivery_fact_v2.dropoff_complete_time_local, eta_at_order_placement_time_local, order_created_time_local | WEEK function applied to the COALESCE hierarchy | None |
| month | integrated_delivery.managed_delivery_fact_v2.dropoff_complete_time_local, eta_at_order_placement_time_local, order_created_time_local | MONTH function applied to the COALESCE hierarchy | None |
| deliverytime_utc | integrated_delivery.managed_delivery_fact_v2.dropoff_complete_time_utc, eta_at_order_placement_time_utc, order_created_time_utc | COALESCE hierarchy similar to date fields but in UTC | None |
| dayofweek | integrated_delivery.managed_delivery_fact_v2.dropoff_complete_time_local, eta_at_order_placement_time_local, order_created_time_local | FORMAT_DATETIME with 'E' format applied to COALESCE hierarchy | None |
| datetime_local | integrated_delivery.managed_delivery_fact_v2.dropoff_complete_time_local, eta_at_order_placement_time_local, order_created_time_local | Direct COALESCE hierarchy without additional transformation | None |
| diner_ty_eta | integrated_delivery.managed_delivery_fact_v2 (multiple timestamp fields) | Complex calculation involving DATE_DIFF between order creation/delivery creation time and lower_bound_eta, converted to minutes | Question: What does "diner_ty_eta" stand for? The logic seems to calculate ETA duration but the business meaning isn't clear from context. |
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
| ghd_late_ind_incl_cancel_time | mdf.ghd_late_ind, cancels.cancel_time_utc, mdf.ghd_eta_utc, mdf.dropoff_complete_time_utc | Complex CASE statement that includes cancellations after ETA as late deliveries when no completion time exists | Question: This seems like an important business rule. Can the logic for including cancelled orders in lateness metrics be documented more clearly? |
| adjustment_reason_name | adj.adjustment_reason_name, adj.adj_contact_reason, cancels.cancel_contact_reason | Extensive CASE statement with REGEXP_LIKE patterns to standardize various reason texts into consistent categories | Question: This standardization logic is quite complex with many hardcoded patterns. Is there a comprehensive mapping document for all these regex patterns? How are new reason types handled? |
| adj_contact_reason | adj.adj_contact_reason | Direct selection from adj CTE | None |
| driver_pay_per_order | integrated_order.order_contribution_profit_fact.driver_pay_per_order | Direct selection from the source table | None |
| tip | integrated_order.order_contribution_profit_fact.tip | Direct selection from the source table | None |
| bundle_ind | mdf.bundle_ind | Direct selection from mdf CTE | None |
| fg_reason | ghg.fg_reason, care_fg.fg_reason, integrated_order.order_contribution_profit_fact.cp_care_concession_awarded_amount | Complex CASE statement with similar REGEXP_LIKE patterns as adjustment_reason_name, but only when concession amount > 0 | Question: The same regex patterns are used here as in adjustment_reason_name. Is there a shared function or reference for these patterns? |
| care_cost_reason | csv_sandbox.care_cost_reasons.care_cost_reason | Direct selection joined on latest_contact_reason | Question: The context mentions this excludes certain contact reasons. What are the specific exclusion criteria? |
| care_cost_group | csv_sandbox.care_cost_reasons.care_cost_group | Uses COALESCE to default to 'not grouped' if null | None |
| cp_care_concession_awarded_amount | integrated_order.order_contribution_profit_fact.cp_care_concession_awarded_amount | Direct selection from the source table | None |
| cp_grub_care_refund | integrated_order.order_contribution_profit_fact.cp_grub_care_refund | Direct selection from the source table | None |
| cp_redelivery_cost | integrated_order.order_contribution_profit_fact.cp_redelivery_cost | Direct selection from the source table | None |
| cp_care_ticket_cost | integrated_order.order_contribution_profit_fact (multiple ticket cost fields) | Sum of cp_diner_care_tickets + cp_driver_care_tickets + cp_restaurant_care_tickets + cp_gh_internal_care_tickets | None |

## CTE: o2
Purpose: Creates a consolidated reason field by prioritizing cancellation reasons over adjustment reasons.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| adjustment_and_cancel_reason_combined | o.cancel_reason_name, o.adjustment_reason_name | CASE statement that uses cancel_reason_name when it's not 'Not Mapped' and not null, otherwise uses adjustment_reason_name | Question: Why is 'Not Mapped' treated the same as null? Is this a known data quality issue or intentional business logic? |

*Note: Most other columns from the o CTE are passed through directly without modification*

## CTE: o3
Purpose: Calculates total care cost and derives final analytical reason groups for aggregation.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| total_care_cost | o2 (multiple cost fields) | Sum of cp_care_concession_awarded_amount + cp_care_ticket_cost + cp_diner_adj + IF(cp_redelivery_cost IS NULL, 0, cp_redelivery_cost) + IF(cp_grub_care_refund IS NULL, 0.00, cp_grub_care_refund) | This is the primary metric calculated by the query |
| cp_redelivery_cost | o2.cp_redelivery_cost | IF statement converting null to 0 | Ensures null values don't break the total_care_cost calculation |
| adjustment_group | o2.cancel_group, integrated_ref.cancellation_reason_map.cancel_group, o2.adjustment_and_cancel_reason_combined | Complex CASE statement using cancellation mapping and REGEXP_LIKE patterns to categorize into 'Logistics Issues', 'Restaurant Issues', 'Diner Issues', or fallback groups | Question: This uses similar regex patterns as the o CTE. Should these be consolidated into a shared reference? |
| fg_group | o2.cancel_group, integrated_ref.cancellation_reason_map.cancel_group, o2.fg_reason | Similar logic as adjustment_group but applied to fg_reason | Same concern about regex pattern duplication |
| fg_reason | o2.cp_care_concession_awarded_amount, o2.fg_reason, o2.adjustment_and_cancel_reason_combined | CASE statement that uses adjustment_and_cancel_reason_combined when concession amount < 0 and fg_reason is null | Question: Why specifically when concession amount is negative? What does a negative concession represent? |

*Note: Most other columns from the o2 CTE are passed through directly*

## Final SELECT
Purpose: Aggregates order-level data into summarized metrics grouped by key analytical dimensions.

| Column Name | Source | Derivation Logic | Notes / Questions |
|-------------|--------|------------------|-------------------|
| cany_ind | o3.CA_Market, o3.NYC_Market | CASE statement that prioritizes CA_Market='CA', then NYC_Market='DCWP', otherwise 'ROM' | [Interpretation] Creates market segments: California, DC/Washington/Philadelphia, Rest of Markets |
| care_cost_reason_group | o3.total_care_cost, o3.adjustment_group, o3.fg_group, o3.care_cost_group | Hierarchical CASE statement starting with zero cost check, then adjustment_group, fg_group, and finally care_cost_group | Question: This hierarchy seems important for business attribution. Is this priority order documented in business requirements? |
| eta_care_reasons | o3.adjustment_and_cancel_reason_combined, o3.fg_reason, o3.care_cost_reason | CASE statement with IN clause checking for specific ETA-related reason text values | Question: The hardcoded list includes some duplicate values. Is this intentional or could it be simplified? |
| orders | o3.order_uuid | COUNT of all order records | Standard row count |
| distinct_order_uuid | o3.order_uuid | COUNT(DISTINCT order_uuid) | Check for potential duplication in the dataset |
| total_care_cost | o3.total_care_cost | SUM of total_care_cost | Primary aggregated metric |
| ghd_orders | o3.ghd_ind | SUM with IF condition counting records where ghd_ind = 'ghd' | Count of Grubhub-delivered orders |
| orders_with_care_cost | o3.cp_diner_adj, o3.cp_care_concession_awarded_amount, o3.cp_care_ticket_cost | SUM with IF condition counting records where the sum of three cost components is negative | Question: Why specifically these three components and not the full total_care_cost? What about redelivery and refund costs? |
| cancels_osmf_definition | o3.order_status_cancel_ind, o3.order_uuid | COUNT with CASE condition counting records where order_status_cancel_ind = TRUE | Question: What does "osmf_definition" stand for? How does this differ from other cancellation indicators in the query? |