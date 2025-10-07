# Detailed Comparison: fulfillment_care_cost.sql (v1) vs LATEST_fulfillment_care_cost.sql

## Executive Summary

This document provides a comprehensive, CTE-by-CTE comparison of two versions of the Fulfillment Care Cost query:
- **v1**: `fulfillment_care_cost.sql` - Original query with date range parameters
- **LATEST**: `LATEST_fulfillment_care_cost.sql` - Updated query with rolling 6-month window

### High-Level Differences

| Aspect | v1 | LATEST | Impact |
|--------|-----|---------|--------|
| **Query Philosophy** | Parameterized date range using `{{start_date}}` and `{{end_date}}` | Fixed rolling 6-month window using `CURRENT_DATE - INTERVAL '6' MONTH` | LATEST is less flexible but ensures consistent lookback period |
| **Number of CTEs** | 10 CTEs | 13 CTEs | LATEST has 3 additional CTEs for enhanced analysis |
| **New CTEs in LATEST** | N/A | `osmf`, `of`, `rest_refunds` | Adds order status milestones, order details, and restaurant refund tracking |
| **Data Granularity** | Order-level before final aggregation | Order-level before final aggregation | Same granularity |
| **Final Output** | Aggregated by market segment and care cost reason group | Aggregated with more dimensions (CBSA, region, delivery type, etc.) | LATEST provides richer dimensional breakdowns |
| **Cost Components** | 5 components: diner_adj, concession, ticket_cost, redelivery, grub_care_refund | 7 components: adds grubcash_concession and restaurant refunds | LATEST captures more complete cost picture |
| **Filters** | GHD only (managed_delivery_ind = TRUE) | Filters by both GHD and delivery indicators | LATEST allows analysis of pickup vs delivery |

### Major Logic Differences

1. **Date Filtering**: v1 uses parameterized dates; LATEST uses rolling 6-month window
2. **New Data Sources**: LATEST adds order status milestones (osmf), order facts (of), and restaurant refunds (rest_refunds)
3. **Enhanced Geography**: LATEST adds CBSA (Core-Based Statistical Area) and tri-state indicators
4. **Additional Metrics**: LATEST tracks shop_and_pay orders, automation deflection, driver true-up costs
5. **Revenue Tracking**: LATEST adds missed revenue calculation for cancelled orders
6. **More Granular Output**: LATEST final SELECT includes more grouping dimensions

---

## CTE-by-CTE Comparison

### 1. CTE: `adj` (Adjustments)

**Purpose**: Identifies orders with Grubhub-paid refunds and retrieves the latest adjustment reason and associated contact reason.

| Column | v1 | LATEST | Notes |
|--------|-----|---------|--------|
| **order_uuid** | `ar.order_uuid` | `ar.order_uuid` | ✅ Identical - Primary key from adjustment_reporting |
| **adjustment_reason_name** | `MAX_BY(reason, adjustment_timestamp_utc)` | `MAX_BY(reason, adjustment_timestamp_utc) AS adjustment_reason_name` | ✅ Identical logic - Gets the latest adjustment reason by timestamp |
| **adj_contact_reason** | `MAX_BY(COALESCE(sr.name, pr.name), adjustment_timestamp_utc)` | `MAX_BY(COALESCE(sr.name, pr.name), adjustment_timestamp_utc) AS adj_contact_reason` | ✅ Identical logic - Gets the latest contact reason, preferring secondary over primary |

**Date Filter Differences**:
| Filter | v1 | LATEST | Notes |
|--------|-----|---------|--------|
| **adjustment_dt** | `BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_ADD('day', 1, DATE '{{end_date}}')` | `>= CURRENT_DATE - INTERVAL '6' MONTH` | ⚠️ **MAJOR DIFFERENCE**: v1 uses parameterized date range with ±1 day buffer; LATEST uses rolling 6-month window |
| **ticket_created_Date** | `BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_ADD('day', 1, DATE '{{end_date}}')` | `>= CURRENT_DATE - INTERVAL '6' MONTH` | ⚠️ **MAJOR DIFFERENCE**: Same pattern - v1 parameterized vs LATEST rolling window |

**Business Logic**: Identical - Both filter for ADJUST_DOWN direction and GRUBHUB payer

**TLDR**: The core logic is identical, but v1 uses flexible date parameters while LATEST uses a fixed 6-month rolling window. This makes v1 more flexible for custom date ranges, while LATEST is simpler for ongoing monitoring with consistent lookback periods.

---

### 2. CTE: `ghg` (Grubhub Guarantee)

**Purpose**: Retrieves orders with granted Grubhub Guarantee claims and categorizes the claim type.

| Column | v1 | LATEST | Notes |
|--------|-----|---------|--------|
| **order_uuid** | `cart_uuid order_uuid` | `cart_uuid AS order_uuid` | ✅ Identical - Renamed from cart_uuid |
| **fg_reason** | `MAX(CASE WHEN claim_type = 'SERVICE' THEN 'Late Delivery - GHG' WHEN claim_type = 'PRICING' THEN 'Price - GHG' END)` | `max(CASE WHEN claim_type = 'SERVICE' THEN 'Late Delivery - GHG' WHEN claim_type = 'PRICING' THEN 'Price - GHG' END) AS fg_reason` | ✅ Identical logic - Categorizes claim type into free grub reason |

**Date Filter Differences**:
| Filter | v1 | LATEST | Notes |
|--------|-----|---------|--------|
| **created_date** | `BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_ADD('day', 1, DATE '{{end_date}}')` | `>= CURRENT_DATE - INTERVAL '6' MONTH` | ⚠️ **MAJOR DIFFERENCE**: v1 uses parameterized range; LATEST uses 6-month window |

**Business Logic**: Identical - Both filter for decision = 'GRANT'

**TLDR**: Core logic identical. Same date filtering difference as adj CTE - v1 flexible, LATEST fixed window.

---

### 3. CTE: `care_fg` (Care Free Grub)

**Purpose**: Identifies concessions (free grub) awarded and retrieves the associated contact reason.

| Column | v1 | LATEST | Notes |
|--------|-----|---------|--------|
| **order_uuid** | `cr.order_uuid` | `cr.order_uuid` | ✅ Identical |
| **fg_reason** | `MAX_BY(COALESCE(sr.name, pr.name), issue_timestamp_utc)` | `MAX_BY(COALESCE(sr.name, pr.name), issue_timestamp_utc) AS fg_reason` | ✅ Identical logic - Latest contact reason by issue timestamp |

**Date Filter Differences**:
| Filter | v1 | LATEST | Notes |
|--------|-----|---------|--------|
| **expiration_dt** | `BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_ADD('day', 1, DATE '{{end_date}}')` | `>= CURRENT_DATE - INTERVAL '6' MONTH` | ⚠️ **MAJOR DIFFERENCE**: v1 parameterized; LATEST 6-month window |
| **ticket_created_Date** | `BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_ADD('day', 1, DATE '{{end_date}}')` | `>= CURRENT_DATE - INTERVAL '6' MONTH` | ⚠️ **MAJOR DIFFERENCE**: Same pattern |

**TLDR**: Identical logic for retrieving free grub reasons. Same consistent date filtering difference across all CTEs.

---

### 4. CTE: `diner_ss_cancels` (Diner Self-Service Cancellations)

**Purpose**: Maps diner self-service cancellation reason codes to human-readable reasons and groups.

| Column | v1 | LATEST | Notes |
|--------|-----|---------|--------|
| **order_uuid** | `order_id order_uuid` | `order_id AS order_uuid` | ✅ Identical - Renamed from order_id |
| **reason_code** | `MAX(reason_code)` | `MAX(reason_code) AS reason_code` | ✅ Identical |
| **diner_ss_cancel_reason** | 5 CASE conditions mapping codes to reasons | 5 CASE conditions mapping codes to reasons | ✅ Identical logic - Maps DINER_PAYMENT_ISSUE, DINER_CHOSE_WRONG_ADDRESS, DINER_CHOSE_WRONG_ORDER_ITEMS, DINER_DOES_NOT_WANT_LATE_ORDER, DINER_DOES_NOT_WANT_THE_FOOD |
| **diner_ss_cancel_reason_group** | 5 CASE conditions mapping to groups | 5 CASE conditions mapping to groups | ✅ Identical logic - Maps to 'Diner Issues' or 'Logistics Issues' |

**Date Filter Differences**:
| Filter | v1 | LATEST | Notes |
|--------|-----|---------|--------|
| **created_date** | `BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_ADD('day', 1, DATE '{{end_date}}')` | `>= CURRENT_DATE - INTERVAL '6' MONTH` | ⚠️ Same pattern - v1 parameterized; LATEST 6-month window |

**TLDR**: Completely identical mapping logic. Only difference is date filtering approach.

---

### 5. CTE: `cancels` (Order Cancellations)

**Purpose**: Retrieves cancelled orders with enriched cancellation reasons from multiple sources.

| Column | v1 | LATEST | Notes |
|--------|-----|---------|--------|
| **order_uuid** | `ocf.order_uuid` | `ocf.order_uuid` | ✅ Identical |
| **order_status_cancel_ind** | `order_status_cancel_ind` | `order_status_cancel_ind` | ✅ Identical |
| **cancel_group** | CASE with fallback to diner_ss and transmission check | CASE with fallback to diner_ss and transmission check | ✅ Identical logic - Uses diner_ss_cancel_reason_group when mapped to 'Not Mapped', checks for transmission issues |
| **cancel_reason_name** | CASE with fallback to diner_ss and transmission check | CASE with same logic | ✅ Identical logic |
| **cancel_pcr** | `pr.name cancel_pcr` | `pr.name AS cancel_pcr` | ✅ Identical - Primary contact reason |
| **cancel_scr** | `sr.name cancel_scr` | `sr.name AS cancel_scr` | ✅ Identical - Secondary contact reason |
| **cancel_time_utc** | `COALESCE(cancellation_time_utc, tf.created_time)` | `COALESCE(cancellation_time_utc, tf.created_time) AS cancel_time_utc` | ✅ Identical |
| **cancel_contact_reason** | `COALESCE(sr.name, pr.name)` | `COALESCE(sr.name, pr.name) AS cancel_contact_reason` | ✅ Identical - Prefers secondary over primary |

**Date Filter Differences**:
| Filter | v1 | LATEST | Notes |
|--------|-----|---------|--------|
| **cancellation_date** | `BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_ADD('day', 1, DATE '{{end_date}}')` | `>= CURRENT_DATE - INTERVAL '6' MONTH` | ⚠️ Same pattern |
| **ticket_created_Date** | `BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_ADD('day', 1, DATE '{{end_date}}')` | `>= CURRENT_DATE - INTERVAL '6' MONTH` | ⚠️ Same pattern |

**TLDR**: Identical business logic for handling cancellation reasons. Consistent date filtering difference.

---

### 6. CTE: `osmf` (Order Status Milestone Fact) - **LATEST ONLY** ✨

**Purpose**: NEW in LATEST - Retrieves order status milestone information, specifically whether an order reached cancelled status.

| Column | LATEST | Notes |
|--------|---------|--------|
| **order_uuid** | `order_uuid` | Primary key |
| **status_cancelled_reached_ind** | `status_cancelled_reached_ind` | Indicator for whether order reached cancelled status |

**Date Filter**: `business_day >= CURRENT_DATE - INTERVAL '6' MONTH`

**TLDR**: This is a NEW CTE in LATEST that provides additional cancellation status information from the order status milestone fact table. This allows differentiation between orders that were formally cancelled vs other types of order issues.

---

### 7. CTE: `of` (Order Fact) - **LATEST ONLY** ✨

**Purpose**: NEW in LATEST - Retrieves order-level dimensional data including CBSA (metro area), delivery region, and large order indicators.

| Column | LATEST | Notes |
|--------|---------|--------|
| **order_uuid** | `of.order_uuid` | Primary key |
| **modified_cbsa_name** | `of.modified_cbsa_name` | Core-Based Statistical Area (metro area) name |
| **key_cities_cbsa** | CASE statement mapping specific CBSAs | Maps to 'New York CBSA Excluding Manhattan', 'New York - Manhattan', 'Chicago-Naperville-Elgin IL-IN-WI', or 'Other CBSA' |
| **ghd_delivery_region_name** | `irmd.ghd_delivery_region_name` | GHD delivery region from merchant dimension |
| **large_order_ind** | CASE statement based on food_and_beverage amount | 'Over $1000', 'Over $250', or 'Less than $250' |

**Date Filter**: `DATE(of.delivery_time_ct) >= CURRENT_DATE - INTERVAL '6' MONTH`

**Joins**: LEFT JOIN to integrated_restaurant.merchant_dim on cust_id

**TLDR**: NEW CTE providing geographic and order size segmentation capabilities. This enables analysis by metro area, delivery region, and order value, which are not available in v1.

---

### 8. CTE: `mdf` (Managed Delivery Fact)

**Purpose**: Retrieves delivery-specific operational data including ETAs, lateness indicators, and timing information.

**⚠️ SIGNIFICANT RESTRUCTURING**: v1 and LATEST have completely different approaches to this CTE.

#### v1 Approach: Detailed Event-Level Data
v1 includes 28 columns with granular delivery event data:

| Column Category | v1 Columns | Purpose |
|----------------|------------|---------|
| **Geography** | region_uuid, region_name, CA_Market, NYC_Market | Market segmentation |
| **Order Attributes** | mealtime, delivery_eta_type, bundle_ind, future | Order characteristics |
| **Dates/Times** | start_of_week, date2, week, month, deliverytime_utc, dayofweek, datetime_local | Temporal analysis dimensions |
| **ETAs** | diner_ty_eta, ghd_eta_utc, dropoff_complete_time_utc | Delivery timing |
| **Indicators** | ghd_late_ind, cancel_ind, cancel_mins | Performance metrics |

**v1 Date Filter**: `business_day BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_ADD('day', 1, DATE '{{end_date}}')`

#### LATEST Approach: Aggregated Order-Level Data  
LATEST uses GROUP BY order_uuid and aggregation functions:

| Column | LATEST Logic | Notes |
|--------|--------------|-------|
| **order_uuid** | `order_uuid` (GROUP BY) | Primary key |
| **dropoff_complete_time_utc** | `MIN_BY(dropoff_complete_time_utc, order_created_time_utc)` | Takes earliest delivery's dropoff time |
| **ghd_eta_utc** | `MIN_BY(eta_at_order_placement_time_utc + INTERVAL '10' MINUTE, order_created_time_utc)` | Takes earliest delivery's ETA +10 min |
| **ghd_late_ind** | `CASE WHEN DATE_DIFF('minute', MIN_BY(...), MIN_BY(...)) > 0 THEN 1 ELSE 0 END` | Calculated from aggregated ETAs |
| **bundle_ind** | `MIN_BY(IF(bundle_type IS NOT NULL, true, false), order_created_time_utc)` | Takes earliest delivery's bundle status |
| **shop_and_pay_ind** | `MIN_BY(IF(delivery_fulfillment_type = 'SHOP_AND_PAY', true, false), order_created_time_utc)` | **NEW** - Shop and pay indicator |

**LATEST Date Filter**: `business_day >= CURRENT_DATE - INTERVAL '6' MONTH`

**MAJOR DIFFERENCES**:
1. **Aggregation**: v1 keeps all delivery records; LATEST aggregates to order level using MIN_BY
2. **Geographic Data**: v1 includes region/market fields; LATEST moves this to separate `of` CTE  
3. **Temporal Fields**: v1 has extensive date/time dimensions; LATEST simplifies to core timing metrics
4. **New Field**: LATEST adds shop_and_pay_ind
5. **Rationale**: LATEST simplifies this CTE and moves dimensional data to dedicated CTEs (of, osmf)

**TLDR**: Major restructuring - v1 is event-level with rich dimensional data; LATEST aggregates to order-level and delegates dimensional data to other CTEs. LATEST adds shop_and_pay tracking. This reflects a fundamental change in query architecture toward more modular CTEs.

---

### 9. CTE: `rest_refunds` (Restaurant Refunds) - **LATEST ONLY** ✨

**Purpose**: NEW in LATEST - Calculates restaurant-initiated refunds from the transaction table.

| Column | LATEST | Notes |
|--------|---------|--------|
| **order_uuid** | `order_uuid` (GROUP BY) | Primary key |
| **rr_refund_total** | `SUM(net_amount) * 0.01` | Total restaurant refunds, converted from cents to dollars |

**Filters**:
- `created_date >= CURRENT_DATE - INTERVAL '6' MONTH`
- `transaction_time_ct >= CURRENT_DATE - INTERVAL '6' MONTH`  
- `transaction_type = 'PCI_SINGLE_REFUND'`

**TLDR**: NEW cost component in LATEST. Captures refunds initiated by restaurants (not Grubhub), providing a more complete picture of total care costs. This is a significant enhancement as v1 doesn't track restaurant-initiated refunds at all.

---

### 10. CTE: `contacts` (Care Contacts)

**Purpose**: Identifies orders with care contacts and retrieves contact reasons and counts.

| Column | v1 | LATEST | Notes |
|--------|-----|---------|--------|
| **order_uuid** | `order_uuid` | `order_uuid` | ✅ Identical |
| **latest_ticket_id** | `MAX_BY(ticket_id, created_time)` | `MAX_BY(ticket_id, created_time) AS latest_ticket_id` | ✅ Identical |
| **latest_contact_reason** | `MAX_BY(COALESCE(sr.name, pr.name), created_time)` | `MAX_BY(COALESCE(sr.name, pr.name), created_time) AS latest_contact_reason` | ✅ Identical |
| **automated_ind** | ❌ Not present | `MAX_BY(automation_deflected_ind, created_time) AS automated_ind` | ✨ **NEW in LATEST** - Tracks whether contact was automated |
| **contacts** | `COUNT(ticket_id) contacts` | `COUNT(ticket_id) AS contacts` | ✅ Identical |

**Date Filter Differences**:
| Filter | v1 | LATEST | Notes |
|--------|-----|---------|--------|
| **ticket_created_date** | `BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_ADD('day', 1, DATE '{{end_date}}')` | `>= CURRENT_DATE - INTERVAL '6' MONTH` | ⚠️ Same pattern |

**Other Filters**: Both filter for `cpo_contact_indicator = 1` (care worked contacts) and `ticket_created_date < CURRENT_DATE`

**TLDR**: Nearly identical. LATEST adds automation tracking via automated_ind field, which enables analysis of automation effectiveness. Same date filtering difference.

---

### 11. CTE: `o` (Main Integration CTE)

**Purpose**: Core integration CTE that combines all previous CTEs with order contribution profit fact and performs reason standardization.

**⚠️ MAJOR STRUCTURAL DIFFERENCES**: This CTE has significant differences in both columns selected and joins performed.

#### Dimensional Columns

| Dimension Category | v1 | LATEST | Notes |
|--------------------|-----|---------|--------|
| **Geographic/Market** | region_uuid, region_name, CA_Market, NYC_Market | modified_cbsa_name, key_cities_cbsa, ghd_delivery_region_name, tri_state_ind | ⚠️ **DIFFERENT**: v1 uses regions; LATEST uses CBSA/metro areas from `of` CTE |
| **Temporal** | date1, start_of_week, week, month, date2, deliverytime_utc, dayofweek, datetime_local, time_local | date1 only | ⚠️ **v1 MORE GRANULAR**: v1 includes extensive temporal dimensions; LATEST keeps only date1 |
| **Delivery Details** | diner_ty_eta, delivery_eta_type, mealtime, cancel_ind, cancel_mins | delivery_ind, cancelled_order_ind, cp_revenue | ⚠️ **DIFFERENT FOCUS**: v1 tracks timing/cancellation details; LATEST adds delivery type and revenue |
| **Order Attributes** | bundle_ind, driver_pay_per_order, tip | bundle_ind, shop_and_pay_ind, driver_pay_per_order, tip, true_up | ✨ LATEST adds shop_and_pay_ind and true_up |
| **New in LATEST** | N/A | status_cancelled_reached_ind, automated_ind, large_order_ind | ✨ NEW dimensions from osmf, contacts, and of CTEs |

#### Core Indicator Columns - Identical Logic

| Column | v1 | LATEST | Notes |
|--------|-----|---------|--------|
| **ghd_ind** | `IF(cpf.managed_delivery_ind = TRUE, 'ghd', 'non-ghd')` | `IF(cpf.managed_delivery_ind=true,'ghd','non-ghd')` | ✅ Identical |
| **order_status_cancel_ind** | `COALESCE(c.order_status_cancel_ind, FALSE)` | `COALESCE(c.order_status_cancel_ind,false)` | ✅ Identical |
| **cancel_fact_ind** | `IF(c.order_uuid IS NOT NULL, TRUE, FALSE)` | `IF(c.order_uuid IS NOT NULL,true,false)` | ✅ Identical |
| **ghd_late_ind** | `COALESCE(ghd_late_ind, 0)` | `COALESCE(ghd_late_ind,0)` | ✅ Identical - from mdf CTE |
| **ghd_late_ind_incl_cancel_time** | CASE with cancel_time logic | CASE with same logic | ✅ Identical logic |

#### Reason Standardization Logic - Identical REGEXP Patterns

Both versions use **identical** REGEXP_LIKE patterns to standardize adjustment_reason_name and fg_reason:

**adjustment_reason_name patterns** (identical in both):
- 'food temp|cold|quality_temp|temperature' → 'food temperature'
- 'incorrect order|incorrect item|wrong order|incorrect_item' → 'incorrect order'
- 'damaged' → 'food damaged'
- 'missing' → 'missing item'
- 'item removed' → 'item removed from order'
- 'late' → 'late order'
- 'menu error' → 'order or menu issue'
- 'temporarily unavailable|unavailable' → 'out of item'
- 'order not rec|missed delivery' → 'missed delivery'

Applies same patterns to both adjustment_reason_name and COALESCE(cancel_contact_reason, adj_contact_reason)

**fg_reason patterns**: Same 9 patterns applied to COALESCE(ghg.fg_reason, care_fg.fg_reason)

**TLDR on Reason Logic**: ✅ **100% IDENTICAL** standardization logic for categorizing adjustment and free grub reasons.

#### Cost Component Columns

| Component | v1 | LATEST | Notes |
|-----------|-----|---------|--------|
| **cp_care_concession_awarded_amount** | Direct from cpf | Direct from cpf | ✅ Identical |
| **cp_grub_care_refund** | Direct from cpf | `COALESCE(cp_grub_care_refund,0)` | ⚠️ LATEST uses COALESCE to default NULL to 0 |
| **cp_redelivery_cost** | Direct from cpf | `COALESCE(cp_redelivery_cost,0)` | ⚠️ LATEST uses COALESCE to default NULL to 0 |
| **cp_care_ticket_cost** | Sum of 4 ticket types | Sum of 4 ticket types | ✅ Identical calculation |
| **cp_grubcash_care_concession_awarded_amount** | ❌ Not present | `COALESCE(cp_grubcash_care_concession_awarded_amount,0)` | ✨ **NEW in LATEST** |
| **true_up** | ❌ Not present | `COALESCE(cp_driver_true_up,0)` | ✨ **NEW in LATEST** - Driver true-up costs |
| **rr_refund** | ❌ Not present | `COALESCE(rr.rr_refund_total,0)` | ✨ **NEW in LATEST** - Restaurant refunds |

#### Join Differences

| Join | v1 | LATEST | Notes |
|------|-----|---------|--------|
| **mdf** | `JOIN mdf` (INNER) | `LEFT JOIN mdf` | ⚠️ v1 requires mdf match; LATEST allows orders without mdf |
| **cancels, adj, ghg, care_fg, contacts** | LEFT JOIN | LEFT JOIN | ✅ Same |
| **care_cost_reasons** | LEFT JOIN | LEFT JOIN | ✅ Same |
| **osmf** | ❌ Not present | LEFT JOIN osmf | ✨ NEW in LATEST |
| **of** | ❌ Not present | LEFT JOIN of | ✨ NEW in LATEST |
| **rest_refunds** | ❌ Not present | LEFT JOIN rest_refunds | ✨ NEW in LATEST |
| **order_location** | ❌ Not present | LEFT JOIN integrated_geo.order_location | ✨ NEW in LATEST |
| **blockgroup_dim** | ❌ Not present | LEFT JOIN integrated_geo.blockgroup_dim | ✨ NEW in LATEST - For tri_state_ind |

#### Filter Differences

| Filter | v1 | LATEST | Notes |
|--------|-----|---------|--------|
| **order_date** | `BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_ADD('day', 1, DATE '{{end_date}}')` | `>= CURRENT_DATE - INTERVAL '6' MONTH` | ⚠️ Same pattern: parameterized vs rolling |
| **managed_delivery_ind** | `= TRUE` (hardcoded) | Not in WHERE clause | ⚠️ **v1 FILTERS TO GHD ONLY**; LATEST includes all orders |
| **delivery_time_ct** | Commented out | `>= CURRENT_DATE - INTERVAL '6' MONTH AND < CURRENT_DATE` | ✨ LATEST adds delivery time filter |

**MAJOR IMPLICATIONS**:
1. **v1 is GHD-only** due to managed_delivery_ind = TRUE filter
2. **LATEST includes all orders** (GHD and non-GHD) but flags them with ghd_ind column
3. LATEST can analyze pickup vs delivery via delivery_ind column

**TLDR**: Reason standardization logic is identical, but v1 focuses on GHD temporal analysis while LATEST provides broader geographic/demographic analysis across all order types with additional cost components.

---

### 12. CTE: `o2` (Adjustment and Cancel Reason Consolidation)

**Purpose**: Creates a combined adjustment/cancellation reason field and passes through most columns from CTE `o`.

#### v1 Columns (24 columns)
Includes dimensional fields from mdf: date1, region_uuid, region_name, CA_Market, NYC_Market, diner_ty_eta, delivery_eta_type, mealtime, month, start_of_week, week, date2, deliverytime_utc, dayofweek, datetime_local, time_local

Plus: ghd_ind, cancel_ind, cancel_mins, order_uuid, cost fields, and derived reasons

#### LATEST Columns (22 columns)  
Includes: date1, ghd_ind, delivery_ind, order_uuid, cost fields, dimensional fields from of CTE (modified_cbsa_name, key_cities_cbsa, ghd_delivery_region_name, large_order_ind, tri_state_ind), status fields (status_cancelled_reached_ind, cancelled_order_ind, automated_ind)

Plus: cp_revenue, true_up, cp_grubcash_care_concession_awarded_amount, rr_refund (NEW cost components)

#### Key Derived Column - Identical Logic

| Column | v1 | LATEST | Notes |
|--------|-----|---------|--------|
| **adjustment_and_cancel_reason_combined** | `CASE WHEN cancel_reason_name = 'Not Mapped' THEN adjustment_reason_name WHEN cancel_reason_name IS NULL THEN adjustment_reason_name ELSE LOWER(cancel_reason_name) END` | Identical CASE logic | ✅ **IDENTICAL** - Prioritizes cancel_reason_name, falls back to adjustment_reason_name |

**TLDR**: Same core logic for combining reasons. Different dimensional columns passed through (v1: temporal/regional; LATEST: geographic/order attributes + new cost fields).

---

### 13. CTE: `o3` (Final Cost Calculation and Grouping)

**Purpose**: Calculates total care cost and derives standardized reason groups for reporting.

#### Total Care Cost Calculation

| Query | Formula | Notes |
|-------|---------|--------|
| **v1** | `cp_care_concession_awarded_amount + cp_care_ticket_cost + cp_diner_adj + IF(cp_redelivery_cost IS NULL, 0, cp_redelivery_cost) + IF(cp_grub_care_refund IS NULL, 0.00, cp_grub_care_refund)` | 5 components |
| **LATEST** | Not explicitly calculated in o3; calculated in final SELECT as `cp_diner_adj+cp_care_concession_awarded_amount+cp_care_ticket_cost+cp_redelivery_cost+cp_grub_care_refund` | 5 components (same) but LATEST also tracks grubcash and rr_refund separately |

**v1 Components**: 
1. cp_care_concession_awarded_amount (free grub)
2. cp_care_ticket_cost (ticket costs)
3. cp_diner_adj (diner adjustments)
4. cp_redelivery_cost (redelivery costs)
5. cp_grub_care_refund (grub care refunds)

**LATEST Adds**:
6. cp_grubcash_care_concession_awarded_amount (grubcash concessions)
7. rr_refund (restaurant refunds)

#### Reason Group Derivation - Similar Logic with Differences

**adjustment_group logic**:

| Condition | v1 | LATEST | Notes |
|-----------|-----|---------|--------|
| When cancel_group exists and != 'Other' | Uses crm.cancel_group | Uses crm.cancel_group | ✅ Identical |
| Pattern: logistics issues | `REGEXP_LIKE(..., 'missed delivery|order not rec|late|damaged|ghd driver|food temp|quality_temp|cold')` → 'Logistics Issues' | Identical pattern | ✅ Identical |
| Pattern: restaurant issues | `REGEXP_LIKE(..., 'missing item|missing|incorrect_order|menu error|incorrect_item|missing_item|incorrect item|incorrect order|quality|special|problem with food|food poison|object in food|temporarily unavailable|out of item|item removed|foreign object')` → 'Restaurant Issues' | Identical pattern | ✅ Identical |
| Pattern: diner issues | `REGEXP_LIKE(..., 'diner error|switch to delivery or pickup|change of plans')` → 'Diner Issues' | Identical pattern | ✅ Identical |
| Else | `COALESCE(crm.cancel_group, 'not grouped')` | Identical | ✅ Identical |

**fg_group logic**: Identical pattern to adjustment_group

**fg_reason backfill logic**:

| Query | Logic | Notes |
|-------|-------|--------|
| **v1** | `CASE WHEN cp_care_concession_awarded_amount < 0 AND fg_reason IS NULL THEN adjustment_and_cancel_reason_combined ELSE fg_reason END` | Backfills fg_reason when concession awarded but no reason |
| **LATEST** | Identical logic | ✅ Identical |

**TLDR**: Reason grouping logic is **100% identical**. Main difference is v1 calculates total_care_cost in o3, while LATEST defers to final SELECT. LATEST tracks additional cost components.

---

### 14. Final SELECT Statement

**Purpose**: Aggregates order-level data to produce final reporting output.

#### v1 Final SELECT - Nested Aggregation

**Structure**: SELECT * FROM (inner SELECT with aggregation)

**Grouping Dimensions** (3):
1. cany_ind (CA/DCWP/ROM market segmentation)
2. care_cost_reason_group (derived from adjustment/fg/care_cost groups)
3. eta_care_reasons (ETA Issues vs Other)

**Metrics** (8):
- orders (COUNT)
- distinct_order_uuid (COUNT DISTINCT)
- total_care_cost (SUM)
- ghd_orders (SUM)
- orders_with_care_cost (SUM)
- cancels_osmf_definition (COUNT)

#### LATEST Final SELECT - Direct Aggregation  

**Structure**: Direct SELECT from o3 with GROUP BY

**Grouping Dimensions** (16):
1. date1
2. ghd_ind
3. delivery_ind ✨
4. cancel_fact_ind
5. adjustment_group
6. adjustment_and_cancel_reason_combined
7. ghd_late_ind
8. ghd_late_ind_incl_cancel_time
9. bundle_ind
10. shop_and_pay_ind ✨
11. automated_ind ✨
12. modified_cbsa_name ✨
13. key_cities_cbsa ✨
14. ghd_delivery_region_name ✨
15. large_order_ind ✨
16. tri_state_ind ✨
17. care_cost_reason_group
18. care_cost_reason

**Metrics** (26):
- orders (COUNT DISTINCT)
- cancels (SUM of cancel_fact_ind)
- missed_revenue_cp (SUM) ✨
- cp_diner_adj (SUM)
- cp_care_concession_awarded_amount (SUM)
- cp_care_ticket_cost (SUM)
- redelivery_cost (SUM)
- cp_grubcash_care_concession_awarded_amount (SUM) ✨
- cp_grubcare_refund (SUM)
- rr_refund (SUM) ✨
- cp_total_care_cost (SUM)
- ghd_late_count (SUM)
- ghd_orders (SUM)
- driver_pay_cpf (SUM) ✨
- tip (SUM) ✨
- true_up (SUM) ✨
- orders_with_adjustments (SUM) ✨
- orders_with_fg (SUM) ✨
- orders_with_redelivery (SUM) ✨
- orders_with_gh_credit (SUM) ✨
- orders_with_gh_credit_refund (SUM) ✨
- orders_with_rr_refund (SUM) ✨
- orders_with_care_cost (SUM)
- cancels_osmf_definition (COUNT)

#### care_cost_reason_group Logic Differences

**v1**:
```sql
LOWER(CASE 
    WHEN total_care_cost = 0 THEN 'orders with no care cost'
    WHEN adjustment_group != 'not grouped' THEN adjustment_group
    WHEN fg_group != 'not grouped' THEN fg_group
    WHEN care_cost_group != 'not grouped' THEN care_cost_group
    ELSE 'not grouped'
END)
```

**LATEST**:
```sql
LOWER(CASE 
    WHEN (cp_diner_adj+cp_care_concession_awarded_amount+cp_care_ticket_cost+cp_redelivery_cost+cp_grub_care_refund) = 0 THEN 'orders with no care cost'
    WHEN cp_redelivery_cost < 0 THEN 'logistics issues'  -- NEW condition
    WHEN adjustment_group != 'not grouped' THEN adjustment_group
    WHEN fg_group != 'not grouped' THEN fg_group
    WHEN care_cost_group != 'not grouped' THEN care_cost_group
    ELSE 'not grouped'
END)
```

⚠️ **LATEST adds**: Explicit 'logistics issues' classification when redelivery cost exists (before checking other groups)

#### care_cost_reason Column - LATEST Only

**LATEST**:
```sql
CASE 
    WHEN cp_redelivery_cost<0 THEN 'Missed Delivery'
    ELSE LOWER(COALESCE(adjustment_and_cancel_reason_combined,fg_reason,care_cost_reason))
END
```

This provides more granular reason detail within each reason group.

**MAJOR DIFFERENCES SUMMARY**:

| Aspect | v1 | LATEST | Impact |
|--------|-----|---------|--------|
| **Granularity** | Aggregated to market/reason level | Order-level with many dimensions | LATEST enables much more detailed slicing |
| **Dimensions** | 3 grouping fields | 18 grouping fields | LATEST 6x more dimensions |
| **Metrics** | 8 metrics | 26 metrics | LATEST 3x more metrics |
| **Cost Components** | 5 components | 7 components tracked separately | LATEST more comprehensive |
| **Geography** | Region-based (CA/NYC/ROM) | CBSA/metro area-based | LATEST more standard geography |
| **Use Case** | High-level monitoring dashboard | Detailed analytical reporting | Different purposes |

**TLDR**: v1 produces a summary dashboard view; LATEST produces a detailed analytical dataset with many more dimensions and metrics for deep-dive analysis.

---

## Summary of Major Differences

### 1. **Date Filtering Philosophy** 
- **v1**: Parameterized dates with {{start_date}} and {{end_date}} parameters, includes ±1 day buffer
- **LATEST**: Fixed 6-month rolling window from CURRENT_DATE
- **Impact**: v1 more flexible for ad-hoc analysis; LATEST consistent for ongoing monitoring

### 2. **Order Scope**
- **v1**: GHD only (managed_delivery_ind = TRUE filter)
- **LATEST**: All orders (GHD and non-GHD, delivery and pickup)
- **Impact**: LATEST has broader coverage and can compare GHD vs non-GHD

### 3. **New CTEs in LATEST** (3 additional)
- **osmf**: Order status milestones (cancellation status)
- **of**: Order facts (CBSA, region, order size)
- **rest_refunds**: Restaurant-initiated refunds
- **Impact**: LATEST captures more data sources and cost components

### 4. **New Cost Components in LATEST** (2 additional)
- **cp_grubcash_care_concession_awarded_amount**: GrubCash concessions
- **rr_refund**: Restaurant refunds
- **Impact**: LATEST has more complete cost picture

### 5. **Geography**
- **v1**: Region-based (CA_Market, NYC_Market)
- **LATEST**: CBSA-based (modified_cbsa_name, key_cities_cbsa) + tri-state indicator
- **Impact**: LATEST uses industry-standard metro area definitions

### 6. **Temporal Dimensions**
- **v1**: Rich temporal fields (start_of_week, week, month, date2, dayofweek, time_local, etc.)
- **LATEST**: Primarily date1
- **Impact**: v1 better for time-of-day and day-of-week analysis

### 7. **Final Output**
- **v1**: 3 dimensions, 8 metrics, highly aggregated summary
- **LATEST**: 18 dimensions, 26 metrics, detailed analytical dataset
- **Impact**: Different use cases - v1 for dashboards, LATEST for analysis

### 8. **Additional Metrics in LATEST**
- Automation indicator (automated_ind)
- Shop and pay indicator (shop_and_pay_ind)
- Driver true-up costs (true_up)
- Large order indicator (large_order_ind)
- Missed revenue for cancellations
- Breakdown by cost component type
- **Impact**: LATEST enables more sophisticated analysis

### 9. **Reason Standardization Logic**
- **100% IDENTICAL** between v1 and LATEST
- Same REGEXP_LIKE patterns
- Same grouping logic
- **Impact**: Ensures comparability of reason classifications

### 10. **Data Architecture**
- **v1**: Monolithic approach with everything in fewer CTEs
- **LATEST**: Modular approach with specialized CTEs
- **Impact**: LATEST more maintainable and extensible

---

## Recommendations for Use

### Use v1 When:
- Need custom date range analysis
- Focused on GHD performance only
- Need time-of-day or day-of-week patterns
- Want simple, aggregated dashboard view
- Analyzing specific region performance (CA, NYC)

### Use LATEST When:
- Need consistent 6-month rolling window
- Comparing GHD vs non-GHD performance
- Analyzing by metro area (CBSA)
- Need comprehensive cost breakdown
- Analyzing shop-and-pay orders
- Tracking restaurant refunds
- Need granular analytical dataset
- Analyzing automation effectiveness
- Need order size segmentation

---

## Conclusion

Both queries serve the same fundamental purpose - calculating Fulfillment Care Costs - but with different philosophies:

- **v1** is optimized for **flexible, focused monitoring** of GHD performance with rich temporal analysis
- **LATEST** is optimized for **comprehensive, standardized reporting** across all order types with rich dimensional analysis

The core logic for reason standardization and grouping is identical, ensuring consistency in how care costs are categorized. The main differences are in scope (GHD-only vs all orders), geography (regions vs CBSAs), cost components (5 vs 7), and output granularity (summary vs detailed).

Neither is strictly "better" - they serve different analytical needs within the organization.

