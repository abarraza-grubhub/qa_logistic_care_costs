# SQL JOIN Order Analysis Report
==================================================

## Summary
- Total JOINs analyzed: 21
- Tables identified: 17
- Critical issues: 0
- High priority optimization opportunities: 1
- Medium priority optimization opportunities: 0
- Informational notes: 0

## Table Size Categories

### Large Tables:
- adjustment_reporting (CTE)
- concession_reporting (CTE)
- order_cancellation_fact (CTE)
- order_contribution_profit_fact (CTE)
- ticket_fact

### Medium Tables:
- cancellation_reason_map

### Small Tables:
- care_cost_reasons
- primary_contact_reason
- secondary_contact_reason

### Derived Tables:
- adj (CTE)
- care_fg (CTE)
- contacts (CTE)
- ghg (CTE)
- mdf (CTE)
- o2 (CTE)

### Unknown Tables:
- cancels (CTE)
- diner_ss_cancels (CTE)

## JOIN Analysis Details

**Line 8:** `adjustment_reporting LEFT JOIN ticket_fact`
- Left table size: large
- Right table size: large
- Join condition: `tf.ticket_id = ar.ticket_id AND tf.ticket_created_Date BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_AD...`
- Performance impact: Low
- ✅ **Status**: JOIN order appears optimal

**Line 12:** `ticket_fact LEFT JOIN primary_contact_reason`
- Left table size: large
- Right table size: small
- Join condition: `pr.contact_reason_id = tf.primary_contact_reason`
- Performance impact: Low
- ✅ **Status**: JOIN order appears optimal

**Line 15:** `primary_contact_reason LEFT JOIN secondary_contact_reason`
- Left table size: small
- Right table size: small
- Join condition: `sr.contact_reason_id = tf.secondary_contact_reason`
- Performance impact: Low
- ✅ **Status**: JOIN order appears optimal

### CTE: care_fg

**Line 48:** `concession_reporting LEFT JOIN ticket_fact`
- Left table size: large
- Right table size: large
- Join condition: `tf.ticket_id = cr.ticket_id AND tf.ticket_created_Date BETWEEN DATE_ADD('day', -1, DATE '{{start_date}}') AND DATE_AD...`
- Performance impact: Low
- ✅ **Status**: JOIN order appears optimal

**Line 52:** `ticket_fact LEFT JOIN primary_contact_reason`
- Left table size: large
- Right table size: small
- Join condition: `pr.contact_reason_id = tf.primary_contact_reason`
- Performance impact: Low
- ✅ **Status**: JOIN order appears optimal

**Line 55:** `primary_contact_reason LEFT JOIN secondary_contact_reason`
- Left table size: small
- Right table size: small
- Join condition: `sr.contact_reason_id = tf.secondary_contact_reason`
- Performance impact: Low
- ✅ **Status**: JOIN order appears optimal

### CTE: cancels

**Line 112:** `order_cancellation_fact LEFT JOIN cancellation_reason_map`
- Left table size: large
- Right table size: medium
- Join condition: `CAST(crm.cancel_reason_id AS VARCHAR) = CAST(ocf.primary_cancel_reason_id AS VARCHAR)`
- Performance impact: Low
- ✅ **Status**: JOIN order appears optimal

**Line 115:** `cancellation_reason_map LEFT JOIN ticket_fact`
- Left table size: medium
- Right table size: large
- Join condition: `tf.ticket_id = CAST(ocf.cancellation_ticket_id AS BIGINT) AND tf.ticket_created_Date BETWEEN DATE_ADD('day', -1, DATE...`
- Performance impact: High - can significantly reduce query execution time
- ⚠️  **HIGH PRIORITY**: Large table (ticket_fact) should typically be on the left side of JOIN for better performance
- 💡 **Suggested**: `ticket_fact LEFT JOIN cancellation_reason_map`

**Line 119:** `ticket_fact LEFT JOIN primary_contact_reason`
- Left table size: large
- Right table size: small
- Join condition: `pr.contact_reason_id = tf.primary_contact_reason`
- Performance impact: Low
- ✅ **Status**: JOIN order appears optimal

**Line 122:** `primary_contact_reason LEFT JOIN secondary_contact_reason`
- Left table size: small
- Right table size: small
- Join condition: `sr.contact_reason_id = tf.secondary_contact_reason`
- Performance impact: Low
- ✅ **Status**: JOIN order appears optimal

**Line 125:** `secondary_contact_reason LEFT JOIN diner_ss_cancels`
- Left table size: small
- Right table size: unknown
- Join condition: `ccr.order_uuid = ocf.order_uuid`
- Performance impact: Low
- ✅ **Status**: JOIN order appears optimal

### CTE: contacts

**Line 192:** `ticket_fact LEFT JOIN primary_contact_reason`
- Left table size: large
- Right table size: small
- Join condition: `pr.contact_reason_id = tf.primary_contact_reason`
- Performance impact: Low
- ✅ **Status**: JOIN order appears optimal

**Line 195:** `primary_contact_reason LEFT JOIN secondary_contact_reason`
- Left table size: small
- Right table size: small
- Join condition: `sr.contact_reason_id = tf.secondary_contact_reason`
- Performance impact: Low
- ✅ **Status**: JOIN order appears optimal

### CTE: o

**Line 291:** `order_contribution_profit_fact JOIN mdf`
- Left table size: large
- Right table size: derived
- Join condition: `mdf.order_uuid = cpf.order_uuid`
- Performance impact: Low
- ✅ **Status**: JOIN order appears optimal

**Line 294:** `mdf LEFT JOIN cancels`
- Left table size: derived
- Right table size: unknown
- Join condition: `c.order_uuid = cpf.order_uuid`
- Performance impact: Low
- ✅ **Status**: JOIN order appears optimal

**Line 297:** `cancels LEFT JOIN adj`
- Left table size: unknown
- Right table size: derived
- Join condition: `adj.order_uuid = cpf.order_uuid`
- Performance impact: Low
- ✅ **Status**: JOIN order appears optimal

**Line 300:** `adj LEFT JOIN ghg`
- Left table size: derived
- Right table size: derived
- Join condition: `ghg.order_uuid = cpf.order_uuid`
- Performance impact: Low
- ✅ **Status**: JOIN order appears optimal

**Line 303:** `ghg LEFT JOIN care_fg`
- Left table size: derived
- Right table size: derived
- Join condition: `care_fg.order_uuid = cpf.order_uuid`
- Performance impact: Low
- ✅ **Status**: JOIN order appears optimal

**Line 306:** `care_fg LEFT JOIN contacts`
- Left table size: derived
- Right table size: derived
- Join condition: `contacts.order_uuid = cpf.order_uuid`
- Performance impact: Low
- ✅ **Status**: JOIN order appears optimal

**Line 309:** `contacts LEFT JOIN care_cost_reasons`
- Left table size: derived
- Right table size: small
- Join condition: `ccr.scr = contacts.latest_contact_reason`
- Performance impact: Low
- ✅ **Status**: JOIN order appears optimal

### CTE: o3

**Line 428:** `o2 LEFT JOIN cancellation_reason_map`
- Left table size: derived
- Right table size: medium
- Join condition: `LOWER(crm.cancel_reason_name) = LOWER(o2.adjustment_and_cancel_reason_combined)`
- Performance impact: Low
- ✅ **Status**: JOIN order appears optimal

## Key Recommendations

### ⚠️ High Priority Issues
- **Line 115** in CTE 'cancels': Large table (ticket_fact) should typically be on the left side of JOIN for better performance

## Notes

- This analysis is based on heuristics derived from table naming conventions
- Actual table sizes may vary and should be verified with database statistics
- JOIN order optimization impact depends on the query optimizer and database system
- Consider creating database statistics or using EXPLAIN PLAN for definitive optimization