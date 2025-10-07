**A complete and updated version, with comments and images, can be found in Google Docs:
[here](https://docs.google.com/document/d/1O6tZxbNd6mV2a_-JMTF1q6qXiZMxvkdw3HEBPdSIkws/edit?usp=sharing).**

# **Breaking Down LATEST Logistic Care Costs Query**

Document Author: [Andrea Barraza](mailto:abarraza@grubhub.com)  
Team: Research and Experimentation (REX)  
Type of Document: RFC  
Last Update: Jun 2, 2025  
Project: [Automation of the Driver Pay Experiment Readout](https://docs.google.com/document/u/1/d/1ElWzi8vgWXAbiZDgYMAL5EgNOl8SRcWwdqKZiangovo/edit)

1. # Introduction

This is a RFC document created to align on the logic and interpretation of the LATEST [Logistic Care Costs](https://tableau.grubhub.com/#/site/Care/workbooks/6887/views) query. It is intended to support async collaboration between the Experimentation Data Science (DS) team and Finance team. 

**Goals**

* Ensure the DS team is aligned with the stakeholder's interpretation of what the query is doing.  
  * Provide a clear breakdown of the query's components, inputs, and outputs to demonstrate alignment with its intended logic and structure.  
* Identify and address any open questions. Questions will be added as comments in the doc.  
* Document shared understanding as part of [Automation of the Driver Pay Experiment Readout](https://docs.google.com/document/u/1/d/1ElWzi8vgWXAbiZDgYMAL5EgNOl8SRcWwdqKZiangovo/edit) project.

**Secondary Goals:**

* The document can serve as a tool to help interpret and understand the query. Most sections are best read alongside the query, which provides helpful context and makes it easier to follow how each part fits together.

The methodology employed to put this document together is found in [Appendix A](#bookmark=id.6f1hmvzgkrjq).

2. # Query Goal and Context

This SQL query (found in `LATEST_fulfillment_care_cost.sql`) is designed to calculate Fulfillment Care Costs (FCC) at Grubhub.

FCC specifically refers to expenses incurred due to issues caused directly from the order fulfillment process – from the moment an order is placed to when it reaches the diner's door. These encompass costs from customer care interactions, compensations such as refunds, and operational responses like redeliveries, all triggered by fulfillment problems, such as late deliveries, missing or incorrect items, or food quality issues upon arrival.

For context, FCC is a core performance indicator contributing to the calculation of Total Fulfillment Costs, a key business metric tracked by the company.

The primary financial metric this query computes is total\_care\_cost (more details in [Section 7](#bookmark=id.tz48ues0uvxb)). The query also categorizes these costs into key analytical groups, including the primary reasons for care incidents and specific market segments.

By leveraging this structured output, stakeholders can effectively analyze FCC patterns and trends. This enables comparisons across different dimensions—such as various markets, distinct timeframes, and specific care reason categories—to pinpoint areas for operational improvement. For instance, consistently high costs attributed to 'late delivery' reasons can highlight a need to investigate and optimize delivery speed or ETA accuracy.

3. # Query High-Level Logic

The query calculates Fulfillment Care Costs (FCC) by processing order-level data through several key tasks:

* **Identifies relevant orders** that experienced issues such as refunds, cancellations, guarantee claims, concessions, or customer contacts, using multiple data sources that include associated reasons.

* **Integrates data across all orders** (all GH delivered orders), combining delivery details (e.g., ETAs, actual delivery times, lateness flags, market segmentation) with associated care data and financial cost components.

* **Standardizes and categorizes issue reasons** using logic that consolidates raw reason fields into consistent fulfillment-related categories (e.g., 'late order', 'incorrect item'), which are later refined into broader analytical groups (e.g., 'logistics issues', 'transmission issues').

* **Calculates total\_care\_cost** by summing cost components tied to the identified issues, such as concessions, adjustments, and care ticket handling.

* **Aggregates results** by key dimensions like reason categories, market segment, and specific ETA-related flags, producing a summarized view of care costs for analysis and reporting.

Much of the query's complexity lies in collecting care cost reasons from multiple sources and classifying them through this multi-step process, enabling clear attribution to fulfillment-related failures —such as logistics issues— versus other causes.

4. # CTE Description

The query is structured using multiple Common Table Expressions (CTEs) to break down the complex logic. Each CTE builds upon the previous ones or source tables to progressively refine the data and lead towards the final calculation of FCC.

The following table provides a brief summary of each CTE in the order of its appearance:

| CTE Name | Description |
| ----- | ----- |
| adj | Identifies orders with Grubhub-paid refunds (adjustments where direction \= 'ADJUST\_DOWN' and payer \= 'GRUBHUB').  This CTE focuses on the 'latest' adjustment for each order, determined by the record with the maximum adjustment\_timestamp\_utc. For this latest adjustment, it retrieves: The adjustment\_reason\_name, which is the formally recorded cause sourced from the adjustment record itself. The adj\_contact\_reason, sourced from an associated care ticket if one is linked to the adjustment. The adj\_contact\_reason reflects what was noted during the customer service contact, while the adjustment\_reason\_name is the official reason logged for the adjustment transaction itself. |
| ghg | Identifies orders with granted Grubhub Guarantee (GHG) claims (decision \= 'GRANT').  Based on the claim\_type, it then categorizes the claim's reason into the fg\_reason field as either 'Late Delivery \- GHG' or 'Price \- GHG' |
| care\_fg | Identifies orders that had concessions. For these orders, it populates the fg\_reason field with the latest customer contact reason associated with the concession(s), sourced from a linked care ticket. |
| diner\_ss\_cancels | Identifies orders with diner self-service cancellations. For each order, it selects a representative reason\_code and maps it to a descriptive diner\_ss\_cancel\_reason and a broader diner\_ss\_cancel\_reason\_group. |
| cancels | Identifies orders that have been cancelled, and retrieves key details like their cancellation time and status. For each order, it determines the cancel\_group and cancel\_reason\_name based on the primary cancellation data, diner self-service cancellation requests and any linked care tickets. If care tickets are linked, it also retrieves the primary (cancel\_pcr) and secondary (cancel\_scr) contact reasons, and then derives a single cancel\_contact\_reason based on those. |
| osmf | Retrieves the cancellation status indicator (status\_cancelled\_reached\_ind) for orders from the order status milestone fact table. This provides an additional source of cancellation information beyond the cancellation fact table. |
| of | Retrieves order-level contextual data including CBSA (Core Based Statistical Area) information, categorizes key cities (New York, Chicago), identifies large orders based on food and beverage totals, and links to delivery region names from the merchant dimension. |
| mdf | Retrieves simplified delivery metrics for Grubhub-managed deliveries, focusing on the earliest order creation time for multi-delivery scenarios. It calculates key estimated times of arrival (ETAs), delivery completion times, and derives the primary delivery lateness indicator (ghd\_late\_ind). Also identifies bundle orders and shop-and-pay orders. This CTE is more streamlined compared to the original query's mdf CTE. |
| rest\_refunds | Calculates the total restaurant refunds (rr\_refund\_total) for each order by summing PCI single refund transactions. This represents refunds processed through the restaurant payment system. |
| contacts | Identifies orders that had a "worked" care contact (non-automated, cpo\_contact\_indicator \= 1). It retrieves the latest contact reason and provides a total count of such contacts for each order. |
| o | Performs several critical data integration and transformation functions: **Integrates Operational and Financial Data:** Joins operational event details from preceding CTEs (adjustments, cancellations, contacts, deliveries, etc.) with corresponding core financial data from order\_contribution\_profit\_fact. **Defines Analysis Scope:** Filters the dataset to include only GHD using managed\_delivery\_ind \= true. **Standardizes Issue Reasons:** Process and categorizes various raw input reasons related to adjustments, free grub, and associated contacts. This uses regexp\_like expressions for keyword and pattern matching, mapping these original reasons to a consistent set of predefined output categories for the adjustment\_reason\_name and fg\_reason fields.  Examples of these standardized categories include 'food temperature', 'incorrect order', 'late order', and 'missed delivery'.  This standardization supports later analysis of cost drivers and allows grouping into broader categories such as 'Logistics Issues'. **Calculates Specific Costs:** Computes the cp\_care\_ticket\_cost by summing care ticket costs related to the diner, driver, restaurant, and internal GH support. Additionally calculates total\_care\_cost directly in this CTE. **Derives Additional Key Indicators:** Assigns care\_cost\_reason and care\_cost\_group values based on standardized reason categorization logic. **Consolidates Data for Subsequent Processing:** Selects and passes through numerous financial components, enriched delivery details, the newly standardized reasons, and all calculated indicators. This comprehensive dataset is then used for the final aggregation and output. |
| Final Select | The final SELECT takes the processed order-level data from the o CTE and summarizes it by first creating grouping dimensions, then calculating aggregated metrics for each group. It defines key analytical dimensions—key\_cities\_cbsa (city segmentation), care\_cost\_reason\_group (category for the care reason), and shop\_and\_pay\_ind (shop and pay order flag)—then calculates aggregated metrics for each combination, including total\_care\_cost, various cost components, order counts, and cancellation metrics. More details on the final output in [Section 8](#bookmark=id.dvarsg89u42r). |

5. # Data Sources Overview

**Unique Data Source Tables:**

* source\_cass\_rainbow\_data.adjustment\_reporting  
  * Contains records of order adjustments (e.g., partial refunds), including amounts, reasons, timestamps, and payer information.

* integrated\_core.ticket\_fact  
  * Stores information about customer care tickets, including creation dates, contact reasons, and associated order IDs.

* source\_zendesk\_ref.primary\_contact\_reason  
  * Reference table for primary contact reasons from Zendesk, mapping reason IDs to human-readable names.

* source\_zendesk\_ref.secondary\_contact\_reason  
  * Reference table for secondary contact reasons from Zendesk, mapping reason IDs to human-readable names.

* ods.carecontactfacade\_guarantee\_claim  
  * Contains data on Grubhub Guarantee (GHG) claims, including claim type, decision (e.g., granted/denied), and creation date.

* source\_cass\_rainbow\_data.concession\_reporting  
  * Records concessions (e.g., "Free Grub") issued, including associated order and ticket IDs, and relevant timestamps.

* ods.carereporting\_cancellation\_result  
  * Stores information about diner self-service cancellation attempts and the reasons provided by the diner.

* integrated\_order.order\_cancellation\_fact  
  * Fact table containing details about order cancellations, including primary cancellation reasons and associated ticket IDs.

* integrated\_ref.cancellation\_reason\_map  
  * Reference table mapping cancellation reason IDs to standardized cancellation groups and descriptive names.

* integrated\_order.order\_status\_milestone\_fact  
  * Tracks order status milestones including cancellation status indicators.

* integrated\_core.order\_fact  
  * Core fact table with order-level information including CBSA details and order totals.

* integrated\_restaurant.merchant\_dim  
  * Dimension table containing restaurant/merchant information including delivery region names.

* integrated\_delivery.managed\_delivery\_fact\_v2  
  * Comprehensive fact table with rich details for Grubhub-managed delivery orders (GHD).

* ods.transaction  
  * Transaction table containing payment and refund records, including restaurant refunds processed through PCI.

* integrated\_order.order\_contribution\_profit\_fact  
  * Serves as the primary financial data source for orders. Supplies the cost components used for calculating Fulfillment Care Costs (FCC).  
    * Contains the financial values that quantify the monetary impact of operational events. For example, cp\_diner\_adj represents the value of diner adjustments. The specific events and their underlying reasons are identified in other tables and then linked to these financial values.

* csv\_sandbox.care\_cost\_reasons  
  * A custom mapping table, used to link specific contact reasons from tickets to standardized care cost reasons and groups.

| CTE Name | Data Sources (Tables and other CTEs) |
| ----- | ----- |
| adj | source\_cass\_rainbow\_data.adjustment\_reporting integrated\_core.ticket\_fact source\_zendesk\_ref.primary\_contact\_reason source\_zendesk\_ref.secondary\_contact\_reason |
| ghg | ods.carecontactfacade\_guarantee\_claim |
| care\_fg | source\_cass\_rainbow\_data.concession\_reporting integrated\_core.ticket\_fact source\_zendesk\_ref.primary\_contact\_reason source\_zendesk\_ref.secondary\_contact\_reason |
| diner\_ss\_cancels | ods.carereporting\_cancellation\_result |
| cancels | integrated\_order.order\_cancellation\_fact integrated\_ref.cancellation\_reason\_map integrated\_core.ticket\_fact source\_zendesk\_ref.primary\_contact\_reason source\_zendesk\_ref.secondary\_contact\_reason diner\_ss\_cancels (CTE)  |
| osmf | integrated\_order.order\_status\_milestone\_fact |
| of | integrated\_core.order\_fact integrated\_restaurant.merchant\_dim |
| mdf | integrated\_delivery.managed\_delivery\_fact\_v2 |
| rest\_refunds | ods.transaction |
| contacts | integrated\_core.ticket\_fact source\_zendesk\_ref.primary\_contact\_reason source\_zendesk\_ref.secondary\_contact\_reason |
| o | integrated\_order.order\_contribution\_profit\_fact mdf (CTE) cancels (CTE) adj (CTE) ghg (CTE) care\_fg (CTE) contacts (CTE) osmf (CTE) of (CTE) rest\_refunds (CTE) csv\_sandbox.care\_cost\_reasons |
| Final Select | o (CTE) |

See [Appendix C](#bookmark=id.cw22isy3d1vo) for details on how key fields from each data source are used in the query's CTEs.

6. # Query Input Parameters: How Dates Are Used in CTEs

Unlike the original fulfillment\_care\_cost.sql query which uses parameterized date inputs ({{start\_date}} and {{end\_date}}), this LATEST query uses a **rolling 6-month window** from the current date.

This query is designed to analyze fulfillment care costs over the most recent 6-month period, automatically calculated as:

* **Start of period**: `CURRENT_DATE - INTERVAL '6' MONTH`  
* **End of period**: `CURRENT_DATE`

**Key Differences from the Original Query:**

1. **No Date Parameters**: The LATEST query does not accept user-provided date parameters. Instead, it always analyzes the most recent 6 months of data.

2. **No +/- 1 Day Logic**: Unlike the original query which often expanded the date range by one day on each side, the LATEST query uses a straightforward `>=` comparison against the 6-month threshold.

3. **Simplified Date Filtering**: All CTEs use consistent date filtering with `>= CURRENT_DATE - INTERVAL '6' MONTH`, making the query more uniform in its date handling.

The table below shows which data sources are filtered by date in each Common Table Expression (CTE):

| CTE Name | Data Source Table Name | Date Column Used in Filter | Date Filter Logic |
| ----- | ----- | ----- | ----- |
| adj | source\_cass\_rainbow\_data.adjustment\_reporting | adjustment\_dt (parsed from string) | \>= CURRENT\_DATE \- INTERVAL '6' MONTH |
| adj | integrated\_core.ticket\_fact | ticket\_created\_date | \>= CURRENT\_DATE \- INTERVAL '6' MONTH |
| ghg | ods.carecontactfacade\_guarantee\_claim | created\_date | \>= CURRENT\_DATE \- INTERVAL '6' MONTH |
| care\_fg | source\_cass\_rainbow\_data.concession\_reporting | expiration\_dt (parsed from string) | \>= CURRENT\_DATE \- INTERVAL '6' MONTH |
| care\_fg | integrated\_core.ticket\_fact | ticket\_created\_date | \>= CURRENT\_DATE \- INTERVAL '6' MONTH |
| diner\_ss\_cancels | ods.carereporting\_cancellation\_result | created\_date | \>= CURRENT\_DATE \- INTERVAL '6' MONTH |
| cancels | integrated\_order.order\_cancellation\_fact | cancellation\_date | \>= CURRENT\_DATE \- INTERVAL '6' MONTH |
| cancels | integrated\_core.ticket\_fact | ticket\_created\_date | \>= CURRENT\_DATE \- INTERVAL '6' MONTH |
| osmf | integrated\_order.order\_status\_milestone\_fact | business\_day | \>= CURRENT\_DATE \- INTERVAL '6' MONTH |
| of | integrated\_core.order\_fact | delivery\_time\_ct (DATE applied) | \>= CURRENT\_DATE \- INTERVAL '6' MONTH |
| mdf | integrated\_delivery.managed\_delivery\_fact\_v2 | business\_day | \>= CURRENT\_DATE \- INTERVAL '6' MONTH |
| rest\_refunds | ods.transaction | created\_date AND transaction\_time\_ct | \>= CURRENT\_DATE \- INTERVAL '6' MONTH (both columns) |
| contacts | integrated\_core.ticket\_fact | ticket\_created\_date | \>= CURRENT\_DATE \- INTERVAL '6' MONTH |
| o | integrated\_order.order\_contribution\_profit\_fact | order\_date | \>= CURRENT\_DATE \- INTERVAL '6' MONTH |

7. # Calculation of total\_care\_cost

The primary financial metric computed by this query is total\_care\_cost. This metric is designed to quantify the direct financial impact on Grubhub from care issues during the order fulfillment process. 

The total\_care\_cost is calculated at an order-level in the o CTE by summing key financial components. The calculation logic in the LATEST query is similar to the original but computed directly in the o CTE rather than in a separate o3 CTE.

The formula used is:

```sql
total_care_cost = cp_care_concession_awarded_amount
                  + cp_care_ticket_cost
                  + cp_diner_adj
                  + IF(cp_redelivery_cost IS NULL, 0, cp_redelivery_cost)
                  + IF(cp_grub_care_refund IS NULL, 0.00, cp_grub_care_refund)
```

Where:

* **cp\_care\_concession\_awarded\_amount**:   
* This represents the monetary value of concessions, such as "Free Grub" or other credits, awarded through customer care due to an issue with an order. 

* **cp\_care\_ticket\_cost**: Represents the calculated cost of handling a care ticket for an order, including factors like customer care agent time and related resources. It's derived in the o CTE by summing four distinct ticket cost components from the integrated\_order.order\_contribution\_profit\_fact table: cp\_diner\_care\_tickets \+ cp\_driver\_care\_tickets \+ cp\_restaurant\_care\_tickets \+ cp\_gh\_internal\_care\_tickets.

* **cp\_diner\_adj**: This value reflects direct financial adjustments made to a diner's order, commonly for issues like missing or incorrect items, or other partial refunds.

* IF(**cp\_redelivery\_cost** IS NULL, 0, cp\_redelivery\_cost): Represents the cost incurred by Grubhub if an order had to be redelivered to the customer.

* IF(**cp\_grub\_care\_refund** IS NULL, 0.00, cp\_grub\_care\_refund): This component accounts for specific refunds that are processed through care channels and are explicitly funded by Grubhub.

8. # Final Output Description

The final output of the query is an aggregated table that summarizes the order-level data from the o CTE (more on CTEs in [Section 4](#bookmark=id.d3laq9rtvd1t)). Including:

| Field | Description |
| :---- | :---- |
| **Grouping Dimensions** |  |
| key\_cities\_cbsa | Categorizes orders by major CBSA (Core Based Statistical Area) locations. **Unique values:** 'New York CBSA Excluding Manhattan', 'New York - Manhattan', 'Chicago-Naperville-Elgin IL-IN-WI', or 'Other CBSA' |
| care\_cost\_reason\_group | Assigns a single care cost reason group based on the standardized reason categorization. This groups orders into categories like 'logistics issues', 'transmission issues', 'restaurant issues', 'diner issues', 'orders with no care cost', 'other', or 'not grouped'. |
| shop\_and\_pay\_ind | Boolean flag indicating whether the order was a shop-and-pay order. **Unique values:** TRUE or FALSE |
| **Aggregated Metrics per Grouping Dimension** |  |
| orders | The total count of order records. |
| distinct\_order\_uuid | The count of unique orders. |
| total\_care\_cost | The sum of the total\_care\_cost values, representing the primary financial outcome. |
| total\_diner\_adj | The sum of cp\_diner\_adj values (diner adjustments). |
| total\_care\_concession | The sum of cp\_care\_concession\_awarded\_amount (concessions awarded). |
| total\_care\_ticket\_cost | The sum of cp\_care\_ticket\_cost (care ticket handling costs). |
| total\_rest\_refunds | The sum of rr\_refund\_total (restaurant refunds). |
| ghd\_orders | The count of orders identified as Grubhub Delivered (where managed\_delivery\_ind is TRUE). |
| ghd\_late\_orders | The count of orders where the ghd\_late\_ind flag is 1 (order was late). |
| orders\_with\_care\_cost | A specific count of orders where the sum of cp\_diner\_adj, cp\_care\_concession\_awarded\_amount, and cp\_care\_ticket\_cost is less than zero. |
| osmf\_cancels | The count of orders flagged as cancelled according to the status\_cancelled\_reached\_ind field from order status milestone fact. |
| bundle\_orders | The count of orders identified as bundle orders. |
| shop\_and\_pay\_orders | The count of orders identified as shop-and-pay orders. |

# Appendices

# Appendix A. Methodology

This report was created with support from **Gemini** and **GitHub Copilot**. The process was as follows:

**1\. Base Document**

This document is based on the structure and content of "Breaking Down Logistic Care Costs Query.md", which was created for the `fulfillment_care_cost.sql` query. That original document was created with support from Gemini through a process of query formatting, iterative document development, and stakeholder validation.

**2\. Adaptation for LATEST Query**

The LATEST query (`LATEST_fulfillment_care_cost.sql`) is a variation of the original query with several key differences:
- Uses a rolling 6-month window (`CURRENT_DATE - INTERVAL '6' MONTH`) instead of parameterized dates
- Includes additional CTEs (osmf, of, rest_refunds) for enhanced analysis
- Simplified mdf CTE focusing on core delivery metrics
- Different final output groupings and metrics

This documentation was adapted to reflect these differences while maintaining coherence with the original document. The logic and definitions that remain the same between the two queries were preserved to ensure consistency in understanding across both versions.

**3\. Consistency and Coherence**

Where the LATEST query uses the same logic as the original query (such as in the adj, ghg, care_fg, diner_ss_cancels, cancels, and contacts CTEs, with only date filtering differences), the explanations from the original document were leveraged and adapted to ensure accurate and consistent documentation.

# Appendix B. LATEST Fulfillment Care Cost Query

The query is available in the `LATEST_fulfillment_care_cost.sql` file in this repository.

Tableau dashboard: https://tableau.grubhub.com/#/site/Care/workbooks/6887/views

Query Last Updated: 4.28.2025

# Appendix C. Data Sources and Key Fields

Reading the table alongside the query makes it easier to interpret.

| CTE Name | Data Source Table Name | Key Fields Used | Brief Description |
| ----- | ----- | ----- | ----- |
| adj | source\_cass\_rainbow\_data.adjustment\_reporting ar | order\_uuid reason adjustment\_timestamp\_utc ticket\_id adjustment\_dt direction payer | \- Provides order adjustment details.  \- Used to filter for Grubhub-paid refunds (direction \= 'ADJUST\_DOWN', payer \= 'GRUBHUB').  \- Identifies the latest adjustment reason (adjustment\_reason\_name) for an order in the off chance there are multiple reasons. |
| adj | integrated\_core.ticket\_fact tf | ticket\_id ticket\_created\_date primary\_contact\_reason secondary\_contact\_reason | \- Links adjustments to tickets via ticket\_id.  \- Fetches primary\_contact\_reason and secondary\_contact\_reason IDs associated with the adjustment. \- Filtered by ticket\_created\_date \>= CURRENT\_DATE \- INTERVAL '6' MONTH.  |
| adj | source\_zendesk\_ref.primary\_contact\_reason pr | contact\_reason\_id name | \- Provides the textual name for the primary contact reason associated with a ticket. \- Maps tf.primary\_contact\_reason ID with pr.contact\_reason\_id. \- The pr.name (reason description) is then used as a fallback in COALESCE(sr.name, pr.name) to determine the adj\_contact\_reason for an order, based on the latest adjustment.  |
| adj | source\_zendesk\_ref.secondary\_contact\_reason sr | contact\_reason\_id name | \- Provides the textual name for the secondary contact reason associated with a ticket. \- Maps tf.secondary\_contact\_reason ID with sr.contact\_reason\_id. \- The name is coalesced with pr.name to determine the adj\_contact\_reason.  |
| ghg | ods.carecontactfacade\_guarantee\_claim | cart\_uuid (as order\_uuid) claim\_type created\_date decision | \- Provides Grubhub Guarantee (GHG) claim data. \- Filters for granted claims (decision \= 'GRANT') within the 6-month window.  \- Categorizes the fg\_reason as 'Late Delivery \- GHG' or 'Price \- GHG' based on claim\_type.  |
| care\_fg | source\_cass\_rainbow\_data.concession\_reporting cr | order\_uuid ticket\_id issue\_timestamp\_utc expiration\_dt | \- Provides data on concessions (e.g., free grub).  \- Filters concessions based on expiration\_dt within the 6-month window. \- Identifies the latest concession for an order using max\_by on issue\_timestamp\_utc.  |
| care\_fg | integrated\_core.ticket\_fact tf | ticket\_id ticket\_created\_date primary\_contact\_reason secondary\_contact\_reason | \- Links concessions to tickets via ticket\_id.  \- Fetches primary\_contact\_reason and secondary\_contact\_reason IDs associated with the concession. \- Filtered by ticket\_created\_date.  |
| care\_fg | source\_zendesk\_ref.primary\_contact\_reason pr | contact\_reason\_id name | \- Maps primary\_contact\_reason ID to its name.  |
| care\_fg | source\_zendesk\_ref.secondary\_contact\_reason sr | contact\_reason\_id name | \- Maps secondary\_contact\_reason ID to its name. \- The sr.name is coalesced with pr.name to determine the latest fg\_reason (free grub reason). |
| diner\_ss\_cancels | ods.carereporting\_cancellation\_result | order\_id (as order\_uuid) reason\_code created\_date | \- Provides data on diner self-service cancellations. \- Filters cancellations by created\_date within 6-month window.  \- Maps reason\_code to a diner\_ss\_cancel\_reason (e.g., 'Payment Issues', 'Late Order').  \- Maps reason\_code to a broader diner\_ss\_cancel\_reason\_group (e.g., 'Diner Issues', 'Logistics Issues').  |
| cancels | integrated\_order.order\_cancellation\_fact ocf | order\_uuid order\_status\_cancel\_ind primary\_cancel\_reason\_id cancellation\_ticket\_id cancellation\_date cancellation\_time\_utc order\_status\_cancel\_reason | \- Core source for order cancellation data. \- Provides order\_status\_cancel\_ind, primary\_cancel\_reason\_id, and cancellation\_ticket\_id.  \- Filters cancellations by cancellation\_date within 6-month window. \- The text field order\_status\_cancel\_reason is used specifically to identify "Transmission Issues", i.e., when the restaurant did not receive the order. |
| cancels | integrated\_ref.cancellation\_reason\_map crm | cancel\_reason\_id cancel\_group cancel\_reason\_name | \- It's used to map the ocf.primary\_cancel\_reason\_id to a standardized cancel\_group and cancel\_reason\_name.  |
| cancels | integrated\_core.ticket\_fact tf | ticket\_id ticket\_created\_date primary\_contact\_reason secondary\_contact\_reason  created\_time | \- Links cancellation tickets (ocf.cancellation\_ticket\_id) to get associated contact reasons.  \- created\_time is used in COALESCE for cancel\_time\_utc. \- Filtered by ticket\_created\_date.  |
| cancels | source\_zendesk\_ref.primary\_contact\_reason pr | contact\_reason\_id name | \- Provides the name for tf.primary\_contact\_reason(cancel\_pcr) associated with cancellation tickets.  |
| cancels | source\_zendesk\_ref.secondary\_contact\_reason sr | contact\_reason\_id name | \- Provides the name for tf.secondary\_contact\_reason(cancel\_scr).  \- The sr.name is coalesced with pr.name to create cancel\_contact\_reason with the latest contact reason. |
| cancels | diner\_ss\_cancels ccr (CTE) | order\_uuid  diner\_ss\_cancel\_reason\_group  diner\_ss\_cancel\_reason | \- Augments cancellation data with self-service cancellation reasons.  \- If cancel\_group or cancel\_reason\_name from crm is 'Not Mapped', values from diner\_ss\_cancels are used.  |
| osmf | integrated\_order.order\_status\_milestone\_fact | order\_uuid status\_cancelled\_reached\_ind business\_day | \- Provides order status milestone data. \- Retrieves the cancellation status indicator for orders. \- Filtered by business\_day within 6-month window. |
| of | integrated\_core.order\_fact | order\_uuid modified\_cbsa\_name food\_and\_beverage delivery\_time\_ct cust\_id | \- Provides core order information. \- Supplies CBSA (city/region) details. \- Used to categorize key cities and large orders. \- Filtered by delivery\_time\_ct within 6-month window. |
| of | integrated\_restaurant.merchant\_dim irmd | cust\_id ghd\_delivery\_region\_name | \- Provides restaurant/merchant dimension data. \- Linked via cust\_id to get delivery region names. |
| mdf | integrated\_delivery.managed\_delivery\_fact\_v2 | order\_uuid dropoff\_complete\_time\_utc eta\_at\_order\_placement\_time\_utc order\_created\_time\_utc bundle\_type delivery\_fulfillment\_type business\_day | \- Provides delivery metrics for managed deliveries. \- Calculates delivery completion times and ETAs using MIN\_BY aggregation (for multi-delivery scenarios). \- Derives ghd\_late\_ind (lateness indicator). \- Identifies bundle and shop-and-pay orders. \- Filtered by business\_day within 6-month window. |
| rest\_refunds | ods.transaction | order\_uuid net\_amount created\_date transaction\_time\_ct transaction\_type | \- Provides transaction data for refunds. \- Filters for PCI\_SINGLE\_REFUND transaction type. \- Calculates total restaurant refunds per order. \- Filtered by both created\_date and transaction\_time\_ct within 6-month window. |
| contacts | integrated\_core.ticket\_fact tf | order\_uuid ticket\_id created\_time primary\_contact\_reason secondary\_contact\_reason ticket\_created\_date cpo\_contact\_indicator | \- Source for customer care ticket information. \- Identifies the latest\_contact\_reason for an order using max\_by on created\_time.  \- Counts the number of contacts (tickets) per order. \- Filters for care-worked contacts (cpo\_contact\_indicator \= 1) as automated contacts do not have a ticket cost. \- Filtered by ticket\_created\_date. |
| contacts | source\_zendesk\_ref.primary\_contact\_reason pr | contact\_reason\_id name | \- Maps primary\_contact\_reason ID to its name.  |
| contacts | source\_zendesk\_ref.secondary\_contact\_reason sr | contact\_reason\_id name | \- Maps secondary\_contact\_reason ID to its name.  \- The sr.name is coalesced with pr.name to determine the latest\_contact\_reason.  |
| o | integrated\_order.order\_contribution\_profit\_fact cpf | order\_date order\_uuid managed\_delivery\_ind cp\_diner\_adj driver\_pay\_per\_order tip cp\_care\_concession\_awarded\_amount cp\_grub\_care\_refund cp\_redelivery\_cost cp\_diner\_care\_tickets cp\_driver\_care\_tickets cp\_restaurant\_care\_tickets cp\_gh\_internal\_care\_tickets | \- Primary fact table for order-level financial and cost data.  \- Supplies key cost components like cp\_diner\_adj, cp\_care\_concession\_awarded\_amount, cp\_grub\_care\_refund, cp\_redelivery\_cost, and various ticket costs.  \- Filters for orders within the 6-month window using order\_date.  |
| o | mdf (CTE) | order\_uuid dropoff\_complete\_time\_utc ghd\_eta\_utc ghd\_late\_ind bundle\_ind shop\_and\_pay\_ind | \- Joined on order\_uuid. Enriches order data with delivery metrics, lateness indicators, and order type flags. |
| o | cancels (CTE) | order\_uuid order\_status\_cancel\_ind cancel\_group cancel\_reason\_name cancel\_time\_utc cancel\_contact\_reason  | \- Joined on order\_uuid. Adds cancellation details including status, group, reason, and contact reason. |
| o | adj (CTE) | order\_uuid adjustment\_reason\_name  adj\_contact\_reason | \- Joined on order\_uuid. Provides adjustment reasons that are then standardized using pattern matching. |
| o | ghg (CTE) | order\_uuid fg\_reason | \- Joined on order\_uuid. Provides Grubhub Guarantee claim reasons. |
| o | care\_fg (CTE) | order\_uuid fg\_reason | \- Joined on order\_uuid. Provides concession-based "free grub" reasons that are then standardized. |
| o | contacts (CTE) | order\_uuid latest\_contact\_reason | \- Joined on order\_uuid. Provides latest\_contact\_reason from care tickets used for reason mapping. |
| o | osmf (CTE) | order\_uuid status\_cancelled\_reached\_ind | \- Joined on order\_uuid. Provides order status milestone cancellation indicator. |
| o | of (CTE) | order\_uuid modified\_cbsa\_name key\_cities\_cbsa ghd\_delivery\_region\_name large\_order\_ind | \- Joined on order\_uuid. Adds CBSA and geographic categorization, and large order indicators. |
| o | rest\_refunds (CTE) | order\_uuid rr\_refund\_total | \- Joined on order\_uuid. Adds restaurant refund totals. |
| o | csv\_sandbox.care\_cost\_reasons ccr | scr (maps to contacts.latest\_contact\_reason) care\_cost\_reason care\_cost\_group | \- Joined on ccr.scr \= contacts.latest\_contact\_reason.  \- Maps specific secondary contact reasons to standardized care\_cost\_reason and care\_cost\_group for categorization. |
| Final Select | o (CTE) | For Deriving Grouping Dimensions:  To create key\_cities\_cbsa: modified\_cbsa\_name To determine care\_cost\_reason\_group: standardized reason fields (adjustment\_reason\_name, fg\_reason) To identify shop\_and\_pay\_ind: shop\_and\_pay\_ind For Aggregations:  order\_uuid (for counts)  total\_care\_cost, cp\_diner\_adj, cp\_care\_concession\_awarded\_amount, cp\_care\_ticket\_cost, rr\_refund\_total (for sums) managed\_delivery\_ind (to count GHD orders) ghd\_late\_ind (to count late orders) status\_cancelled\_reached\_ind (to count cancellations) bundle\_ind, shop\_and\_pay\_ind (to count order types) | This final block aggregates the detailed, order-level data from the o CTE to produce a summarized report of care cost metrics.  It calculates several key metrics: total orders, distinct orders, sum of total care costs and individual cost components, count of GHD orders, count of late orders, count of orders with any care costs, count of cancellations, and counts of bundle and shop-and-pay orders.  These metrics are grouped by:  1\. key\_cities\_cbsa: CBSA location categorization.  2\. care\_cost\_reason\_group: A high-level categorization of care cost reasons.  3\. shop\_and\_pay\_ind: Flag for shop-and-pay orders. |

