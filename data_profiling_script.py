"""
Data Profiling Script for Optimization Validation Analysis

This script validates hypotheses about data type efficiency and filter optimization
for the fulfillment care costs query. It assumes a dictionary 'cte_dfs' is available
with pandas DataFrames for each CTE, including a 'final_output' key.

The script performs validation checks without connecting to databases or reading files.
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime


def validate_data_types():
    """Validate data type optimization hypotheses."""
    
    print("=== DATA TYPE VALIDATION ===\n")
    
    # Validate ticket_id columns are numeric
    if 'adj' in cte_dfs:
        adj_df = cte_dfs['adj']
        print("1. Validating ticket_id in adj CTE:")
        if 'ticket_id' in adj_df.columns:
            # Check if all values are numeric
            numeric_mask = pd.to_numeric(adj_df['ticket_id'], errors='coerce').notna()
            numeric_percentage = (numeric_mask.sum() / len(adj_df)) * 100
            print(f"   - Numeric values: {numeric_percentage:.2f}%")
            
            if numeric_percentage == 100:
                # Check if values fit in BIGINT range (-2^63 to 2^63-1)
                try:
                    values = pd.to_numeric(adj_df['ticket_id'])
                    bigint_min, bigint_max = -2**63, 2**63 - 1
                    in_range = ((values >= bigint_min) & (values <= bigint_max)).all()
                    print(f"   - Values fit in BIGINT range: {in_range}")
                    print(f"   - Recommendation: Convert to BIGINT for better performance")
                except:
                    print(f"   - Error checking BIGINT range compatibility")
            else:
                non_numeric = adj_df.loc[~numeric_mask, 'ticket_id'].unique()
                print(f"   - Non-numeric values found: {non_numeric[:5]}...")
                print(f"   - Recommendation: Clean data before converting to BIGINT")
        else:
            print("   - ticket_id column not found in adj CTE")
        print()
    
    # Validate adjustment_dt format
    if 'adj' in cte_dfs:
        adj_df = cte_dfs['adj']
        print("2. Validating adjustment_dt format in adj CTE:")
        if 'adjustment_dt' in adj_df.columns:
            # Check YYYYMMDD format
            yyyymmdd_pattern = r'^\d{8}$'
            valid_format = adj_df['adjustment_dt'].astype(str).str.match(yyyymmdd_pattern)
            format_percentage = (valid_format.sum() / len(adj_df)) * 100
            print(f"   - YYYYMMDD format compliance: {format_percentage:.2f}%")
            
            if format_percentage == 100:
                # Try to parse as dates
                try:
                    parsed_dates = pd.to_datetime(adj_df['adjustment_dt'], format='%Y%m%d', errors='coerce')
                    valid_dates = parsed_dates.notna().sum()
                    date_percentage = (valid_dates / len(adj_df)) * 100
                    print(f"   - Valid date values: {date_percentage:.2f}%")
                    print(f"   - Recommendation: Convert to DATE type for better performance")
                except:
                    print(f"   - Error parsing dates")
            else:
                invalid_formats = adj_df.loc[~valid_format, 'adjustment_dt'].unique()
                print(f"   - Invalid formats found: {invalid_formats[:5]}...")
        else:
            print("   - adjustment_dt column not found in adj CTE")
        print()
    
    # Validate expiration_dt format in care_fg
    if 'care_fg' in cte_dfs:
        care_fg_df = cte_dfs['care_fg']
        print("3. Validating expiration_dt format in care_fg CTE:")
        if 'expiration_dt' in care_fg_df.columns:
            yyyymmdd_pattern = r'^\d{8}$'
            valid_format = care_fg_df['expiration_dt'].astype(str).str.match(yyyymmdd_pattern)
            format_percentage = (valid_format.sum() / len(care_fg_df)) * 100
            print(f"   - YYYYMMDD format compliance: {format_percentage:.2f}%")
            
            if format_percentage > 95:  # Allow for some null values
                print(f"   - Recommendation: Convert to DATE type for better performance")
            else:
                print(f"   - Data quality issues found, investigate before conversion")
        else:
            print("   - expiration_dt column not found in care_fg CTE")
        print()
    
    # Validate region_uuid format
    if 'mdf' in cte_dfs:
        mdf_df = cte_dfs['mdf']
        print("4. Validating region_uuid format in mdf CTE:")
        if 'region_uuid' in mdf_df.columns:
            # UUID format: 8-4-4-4-12 hex characters
            uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            valid_uuid = mdf_df['region_uuid'].astype(str).str.lower().str.match(uuid_pattern)
            uuid_percentage = (valid_uuid.sum() / len(mdf_df)) * 100
            print(f"   - Valid UUID format: {uuid_percentage:.2f}%")
            
            if uuid_percentage > 95:
                # Calculate storage efficiency
                avg_string_length = mdf_df['region_uuid'].astype(str).str.len().mean()
                print(f"   - Average string length: {avg_string_length:.1f} characters")
                print(f"   - Recommendation: Consider native UUID type (16 bytes vs ~36 bytes)")
            else:
                invalid_uuids = mdf_df.loc[~valid_uuid, 'region_uuid'].unique()
                print(f"   - Invalid UUIDs found: {invalid_uuids[:3]}...")
        else:
            print("   - region_uuid column not found in mdf CTE")
        print()


def validate_financial_precision():
    """Validate financial column precision requirements."""
    
    print("=== FINANCIAL PRECISION VALIDATION ===\n")
    
    if 'final_output' in cte_dfs:
        final_df = cte_dfs['final_output']
        print("5. Validating total_care_cost precision:")
        
        if 'total_care_cost' in final_df.columns:
            # Check for precision beyond 2 decimal places
            cost_values = final_df['total_care_cost'].dropna()
            
            if len(cost_values) > 0:
                # Check decimal places
                decimal_places = []
                for value in cost_values:
                    if pd.notna(value) and value != 0:
                        # Convert to string and count decimal places
                        str_val = f"{value:.10f}".rstrip('0')
                        if '.' in str_val:
                            decimals = len(str_val.split('.')[1])
                            decimal_places.append(decimals)
                        else:
                            decimal_places.append(0)
                
                if decimal_places:
                    max_decimals = max(decimal_places)
                    avg_decimals = np.mean(decimal_places)
                    print(f"   - Maximum decimal places: {max_decimals}")
                    print(f"   - Average decimal places: {avg_decimals:.2f}")
                    
                    # Check for potential precision issues
                    values_with_many_decimals = sum(1 for d in decimal_places if d > 2)
                    percentage = (values_with_many_decimals / len(decimal_places)) * 100
                    
                    print(f"   - Values with >2 decimal places: {percentage:.2f}%")
                    
                    if max_decimals <= 2:
                        print(f"   - Recommendation: DECIMAL(15,2) would be sufficient")
                    else:
                        print(f"   - Recommendation: DECIMAL(15,{max_decimals}) or investigate precision requirements")
                else:
                    print("   - No valid cost values found for analysis")
            else:
                print("   - No cost values found in dataset")
        else:
            print("   - total_care_cost column not found in final output")
        print()


def analyze_filter_efficiency():
    """Analyze filter pushdown opportunities."""
    
    print("=== FILTER EFFICIENCY ANALYSIS ===\n")
    
    # Analyze managed_delivery_ind filter impact
    if 'mdf' in cte_dfs and 'o' in cte_dfs:
        mdf_df = cte_dfs['mdf']
        o_df = cte_dfs['o']
        
        print("6. Analyzing managed_delivery_ind filter impact:")
        
        # Estimate row count impact if filter was pushed to mdf
        print(f"   - Rows in mdf CTE: {len(mdf_df):,}")
        print(f"   - Rows in o CTE: {len(o_df):,}")
        
        if 'managed_delivery_ind' in o_df.columns:
            ghd_orders = o_df['managed_delivery_ind'].sum() if o_df['managed_delivery_ind'].dtype == bool else (o_df['managed_delivery_ind'] == True).sum()
            ghd_percentage = (ghd_orders / len(o_df)) * 100 if len(o_df) > 0 else 0
            
            print(f"   - GHD orders: {ghd_orders:,} ({ghd_percentage:.1f}%)")
            
            # Estimate potential row reduction
            potential_reduction = len(mdf_df) - ghd_orders
            reduction_percentage = (potential_reduction / len(mdf_df)) * 100 if len(mdf_df) > 0 else 0
            
            print(f"   - Potential row reduction if filter pushed to mdf: {potential_reduction:,} ({reduction_percentage:.1f}%)")
            
            if reduction_percentage > 20:
                print(f"   - Recommendation: Consider pushing managed_delivery_ind filter to mdf CTE")
            else:
                print(f"   - Current filter placement appears reasonable")
        else:
            print("   - managed_delivery_ind column not found for analysis")
        print()
    
    # Analyze null order_uuid impact
    cte_with_order_uuid = ['adj', 'ghg', 'care_fg', 'diner_ss_cancels', 'cancels', 'mdf', 'contacts']
    
    print("7. Analyzing order_uuid null value impact:")
    
    for cte_name in cte_with_order_uuid:
        if cte_name in cte_dfs:
            df = cte_dfs[cte_name]
            if 'order_uuid' in df.columns:
                null_count = df['order_uuid'].isnull().sum()
                null_percentage = (null_count / len(df)) * 100 if len(df) > 0 else 0
                print(f"   - {cte_name}: {null_count:,} null values ({null_percentage:.2f}%)")
    print()


def analyze_join_efficiency():
    """Analyze JOIN operation efficiency."""
    
    print("=== JOIN EFFICIENCY ANALYSIS ===\n")
    
    print("8. Type casting analysis for JOINs:")
    
    # Analyze cancellation reason ID casting
    if 'cancels' in cte_dfs:
        print("   - Cancellation reason ID casting:")
        print("     - Query performs: CAST(crm.cancel_reason_id AS VARCHAR) = CAST(ocf.primary_cancel_reason_id AS VARCHAR)")
        print("     - Recommendation: Standardize both columns to INTEGER for better JOIN performance")
        print()
    
    # Analyze ticket ID casting  
    if 'adj' in cte_dfs or 'cancels' in cte_dfs:
        print("   - Ticket ID casting:")
        print("     - Query performs: CAST(ocf.cancellation_ticket_id AS BIGINT)")
        print("     - Recommendation: Store ticket IDs as BIGINT natively to avoid casting overhead")
        print()
    
    print("9. JOIN selectivity analysis:")
    
    # Analyze the main fact table join in o CTE
    if 'o' in cte_dfs and 'mdf' in cte_dfs:
        o_df = cte_dfs['o']
        mdf_df = cte_dfs['mdf']
        
        print(f"   - Main fact table (cpf) to mdf JOIN:")
        print(f"     - Estimated rows before JOIN: {len(mdf_df):,}")
        print(f"     - Estimated rows after JOIN: {len(o_df):,}")
        
        if len(mdf_df) > 0:
            join_efficiency = (len(o_df) / len(mdf_df)) * 100
            print(f"     - JOIN efficiency: {join_efficiency:.1f}%")
            
            if join_efficiency < 80:
                print(f"     - Consider investigating JOIN conditions for optimization")
        print()


def generate_summary_recommendations():
    """Generate summary of all recommendations."""
    
    print("=== SUMMARY RECOMMENDATIONS ===\n")
    
    recommendations = [
        "1. DATA TYPE OPTIMIZATIONS:",
        "   - Convert ticket_id columns from VARCHAR to BIGINT",
        "   - Convert date string columns (adjustment_dt, expiration_dt) to native DATE type",
        "   - Consider native UUID type for region_uuid if database supports it",
        "   - Use DECIMAL instead of FLOAT for financial calculations",
        "",
        "2. FILTER OPTIMIZATIONS:",
        "   - Consider pushing managed_delivery_ind filter to mdf CTE",
        "   - Add early null filtering for order_uuid in relevant CTEs",
        "   - Maintain current early filtering patterns in adj and ghg CTEs",
        "",
        "3. JOIN OPTIMIZATIONS:",
        "   - Standardize data types across JOINed columns to eliminate casting",
        "   - Store all ID columns as appropriate integer types",
        "   - Review JOIN selectivity for potential performance improvements",
        "",
        "4. VALIDATION PRIORITIES:",
        "   - Verify data quality before implementing type changes",
        "   - Test filter pushdown impact on query performance",
        "   - Validate financial precision requirements with business stakeholders",
        "   - Monitor JOIN performance after type standardization"
    ]
    
    for rec in recommendations:
        print(rec)


# Main execution function
def run_validation():
    """Run all validation checks."""
    
    print("FULFILLMENT CARE COSTS QUERY - OPTIMIZATION VALIDATION ANALYSIS")
    print("=" * 70)
    print()
    
    # Check if cte_dfs dictionary is available
    if 'cte_dfs' not in globals():
        print("ERROR: cte_dfs dictionary not found in environment.")
        print("This script requires a pre-populated cte_dfs dictionary with pandas DataFrames.")
        return
    
    print(f"Found {len(cte_dfs)} CTEs in cte_dfs dictionary:")
    for cte_name in sorted(cte_dfs.keys()):
        if isinstance(cte_dfs[cte_name], pd.DataFrame):
            print(f"  - {cte_name}: {len(cte_dfs[cte_name])} rows, {len(cte_dfs[cte_name].columns)} columns")
        else:
            print(f"  - {cte_name}: Not a DataFrame")
    print()
    
    # Run validation functions
    validate_data_types()
    validate_financial_precision()
    analyze_filter_efficiency()
    analyze_join_efficiency()
    generate_summary_recommendations()
    
    print("Validation analysis complete.")


# Execute if cte_dfs is available
if __name__ == "__main__":
    # Check if running in environment with cte_dfs
    try:
        run_validation()
    except NameError:
        print("Note: This script is designed to run in an environment where 'cte_dfs' dictionary is pre-loaded.")
        print("To use this script:")
        print("1. Load your CTE data into pandas DataFrames")
        print("2. Create a dictionary: cte_dfs = {'adj': adj_df, 'ghg': ghg_df, ..., 'final_output': final_df}")
        print("3. Run this script in that environment")