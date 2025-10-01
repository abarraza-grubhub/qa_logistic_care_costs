#!/usr/bin/env python3
"""
Data Quality Validation Script for Fulfillment Care Cost Query

This script helps identify logical gaps in the query output by validating:
- Date ranges are within specified parameters
- Values appear reasonable (timestamps, monetary amounts)
- Negative values are acceptable where expected
- Data consistency across different dimensions

Based on analysis of fulfillment_care_cost.sql and Breaking Down Logistic Care Costs Query.md
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional
import warnings


class DataQualityValidator:
    """Validates data quality for fulfillment care cost query results."""
    
    def __init__(self, start_date: str, end_date: str):
        """
        Initialize validator with date parameters.
        
        Args:
            start_date: Query start date in YYYY-MM-DD format
            end_date: Query end date in YYYY-MM-DD format
        """
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.validation_results = []
        
    def validate_date_ranges(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Validate that all date columns are within expected ranges.
        
        Based on the query documentation, some CTEs use +/- 1 day logic while others use exact dates.
        """
        issues = []
        
        # Date columns that should use exact date range
        exact_date_columns = ['date1', 'date2']
        
        # Date columns that can extend beyond range (+/- 1 day logic)
        extended_date_columns = []
        
        for col in exact_date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
                
                # Check for dates outside the specified range
                out_of_range = df[
                    (df[col] < self.start_date) | 
                    (df[col] > self.end_date)
                ]
                
                if not out_of_range.empty:
                    issues.append({
                        'type': 'date_range_violation',
                        'column': col,
                        'description': f'{len(out_of_range)} records with {col} outside specified date range',
                        'min_date': out_of_range[col].min(),
                        'max_date': out_of_range[col].max(),
                        'expected_range': f'{self.start_date.date()} to {self.end_date.date()}'
                    })
        
        return issues
    
    def validate_timestamp_logic(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Validate that timestamps make logical sense (e.g., delivery after order creation).
        """
        issues = []
        
        # Check for impossible timestamp relationships
        if 'deliverytime_utc' in df.columns and 'datetime_local' in df.columns:
            # Convert to datetime if they're not already
            df['deliverytime_utc'] = pd.to_datetime(df['deliverytime_utc'])
            df['datetime_local'] = pd.to_datetime(df['datetime_local'])
            
            # Check for cases where delivery time is too far in the future
            future_threshold = self.end_date + timedelta(days=2)
            future_deliveries = df[df['deliverytime_utc'] > future_threshold]
            
            if not future_deliveries.empty:
                issues.append({
                    'type': 'future_timestamp',
                    'column': 'deliverytime_utc',
                    'description': f'{len(future_deliveries)} records with delivery times far in the future',
                    'max_future_date': future_deliveries['deliverytime_utc'].max()
                })
        
        # Check for time zone consistency
        if 'time_local' in df.columns:
            # Validate that time_local is within reasonable bounds (0-24 hours)
            try:
                time_values = pd.to_datetime(df['time_local'], format='%H:%M', errors='coerce')
                invalid_times = df[time_values.isna() & df['time_local'].notna()]
                
                if not invalid_times.empty:
                    issues.append({
                        'type': 'invalid_time_format',
                        'column': 'time_local',
                        'description': f'{len(invalid_times)} records with invalid time format',
                        'sample_values': invalid_times['time_local'].head().tolist()
                    })
            except Exception as e:
                issues.append({
                    'type': 'time_parsing_error',
                    'column': 'time_local',
                    'description': f'Error parsing time_local column: {str(e)}'
                })
        
        return issues
    
    def validate_monetary_values(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Validate monetary amounts for reasonableness.
        """
        issues = []
        
        # Expected monetary columns based on the query
        monetary_columns = [
            'total_care_cost', 'cp_diner_adj', 'cp_care_concession_awarded_amount',
            'driver_pay_per_order', 'tip', 'cp_redelivery_cost'
        ]
        
        for col in monetary_columns:
            if col in df.columns:
                # Check for extreme values that might indicate data issues
                col_data = pd.to_numeric(df[col], errors='coerce')
                
                # Check for unreasonably large positive values
                large_values = col_data[col_data > 10000]  # Over $10,000
                if not large_values.empty:
                    issues.append({
                        'type': 'extreme_monetary_value',
                        'column': col,
                        'description': f'{len(large_values)} records with extremely large values (>${large_values.min():.2f} to ${large_values.max():.2f})',
                        'max_value': large_values.max(),
                        'sample_records': large_values.head().tolist()
                    })
                
                # Check for negative values where they might not be expected
                negative_values = col_data[col_data < 0]
                
                # For some columns, negative values are expected (refunds, adjustments)
                expected_negative_columns = ['cp_diner_adj', 'total_care_cost']
                
                if not negative_values.empty and col not in expected_negative_columns:
                    issues.append({
                        'type': 'unexpected_negative_value',
                        'column': col,
                        'description': f'{len(negative_values)} records with negative values in {col}',
                        'min_value': negative_values.min(),
                        'sample_records': negative_values.head().tolist()
                    })
                
                # Check for NaN values where they shouldn't exist
                nan_count = col_data.isna().sum()
                if nan_count > 0:
                    issues.append({
                        'type': 'missing_monetary_values',
                        'column': col,
                        'description': f'{nan_count} records with missing values in {col}',
                        'percentage': (nan_count / len(df)) * 100
                    })
        
        return issues
    
    def validate_categorical_consistency(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Validate categorical columns for expected values and consistency.
        """
        issues = []
        
        # Expected categorical mappings based on the query
        expected_values = {
            'ghd_ind': ['ghd', 'non-ghd'],
            'CA_Market': ['CA', 'xCA'],
            'NYC_Market': ['DCWP', 'xDCWP'],
            'cany_ind': ['CA', 'DCWP', 'ROM'],
            'eta_care_reasons': ['ETA Issues', 'Other']
        }
        
        for col, expected in expected_values.items():
            if col in df.columns:
                unique_values = df[col].unique()
                unexpected_values = set(unique_values) - set(expected) - {np.nan, None}
                
                if unexpected_values:
                    issues.append({
                        'type': 'unexpected_categorical_value',
                        'column': col,
                        'description': f'Unexpected values in {col}',
                        'unexpected_values': list(unexpected_values),
                        'expected_values': expected
                    })
        
        # Check for consistency between related boolean flags
        if 'cancel_ind' in df.columns and 'order_status_cancel_ind' in df.columns:
            inconsistent = df[
                (df['cancel_ind'] == 1) & (df['order_status_cancel_ind'] == False)
            ]
            
            if not inconsistent.empty:
                issues.append({
                    'type': 'inconsistent_cancel_flags',
                    'description': f'{len(inconsistent)} records with inconsistent cancellation flags',
                    'details': 'cancel_ind=1 but order_status_cancel_ind=False'
                })
        
        return issues
    
    def validate_aggregation_logic(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Validate aggregation logic and check for data consistency.
        """
        issues = []
        
        # Check if orders equals distinct_order_uuid (should be the same)
        if 'orders' in df.columns and 'distinct_order_uuid' in df.columns:
            mismatched = df[df['orders'] != df['distinct_order_uuid']]
            
            if not mismatched.empty:
                issues.append({
                    'type': 'order_count_mismatch',
                    'description': f'{len(mismatched)} records where orders != distinct_order_uuid',
                    'details': 'This suggests potential data duplication issues'
                })
        
        # Check that orders_with_care_cost <= total orders
        if 'orders_with_care_cost' in df.columns and 'orders' in df.columns:
            invalid = df[df['orders_with_care_cost'] > df['orders']]
            
            if not invalid.empty:
                issues.append({
                    'type': 'logical_inconsistency',
                    'description': f'{len(invalid)} records where orders_with_care_cost > total orders',
                    'details': 'Care cost orders cannot exceed total orders'
                })
        
        # Check that GHD orders are reasonable
        if 'ghd_orders' in df.columns and 'orders' in df.columns:
            invalid_ghd = df[df['ghd_orders'] > df['orders']]
            
            if not invalid_ghd.empty:
                issues.append({
                    'type': 'logical_inconsistency',
                    'description': f'{len(invalid_ghd)} records where ghd_orders > total orders',
                    'details': 'GHD orders cannot exceed total orders'
                })
        
        return issues
    
    def validate_business_logic(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Validate business logic specific to fulfillment care costs.
        """
        issues = []
        
        # Check that records with 'orders with no care cost' actually have zero total_care_cost
        if 'care_cost_reason_group' in df.columns and 'total_care_cost' in df.columns:
            no_cost_records = df[df['care_cost_reason_group'] == 'orders with no care cost']
            non_zero_cost = no_cost_records[no_cost_records['total_care_cost'] != 0]
            
            if not non_zero_cost.empty:
                issues.append({
                    'type': 'business_logic_violation',
                    'description': f'{len(non_zero_cost)} records marked as "no care cost" but have non-zero total_care_cost',
                    'sample_costs': non_zero_cost['total_care_cost'].head().tolist()
                })
        
        # Check for reasonable ETA values (should be positive for most cases)
        if 'diner_ty_eta' in df.columns:
            df['diner_ty_eta'] = pd.to_numeric(df['diner_ty_eta'], errors='coerce')
            extreme_negative_eta = df[df['diner_ty_eta'] < -1440]  # Less than -24 hours
            
            if not extreme_negative_eta.empty:
                issues.append({
                    'type': 'extreme_eta_value',
                    'column': 'diner_ty_eta',
                    'description': f'{len(extreme_negative_eta)} records with extremely negative ETA values',
                    'min_value': extreme_negative_eta['diner_ty_eta'].min(),
                    'details': 'ETA values less than -24 hours may indicate data issues'
                })
        
        return issues
    
    def run_validation(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Run all validation checks on the dataframe.
        
        Args:
            df: DataFrame containing query results
            
        Returns:
            Dictionary containing validation results
        """
        print(f"Starting data quality validation for {len(df)} records...")
        print(f"Date range: {self.start_date.date()} to {self.end_date.date()}")
        print(f"Columns available: {list(df.columns)}")
        print("-" * 50)
        
        all_issues = []
        
        # Run all validation checks
        validation_methods = [
            ('Date Range Validation', self.validate_date_ranges),
            ('Timestamp Logic Validation', self.validate_timestamp_logic),
            ('Monetary Values Validation', self.validate_monetary_values),
            ('Categorical Consistency Validation', self.validate_categorical_consistency),
            ('Aggregation Logic Validation', self.validate_aggregation_logic),
            ('Business Logic Validation', self.validate_business_logic)
        ]
        
        for check_name, method in validation_methods:
            print(f"Running {check_name}...")
            try:
                issues = method(df)
                all_issues.extend(issues)
                print(f"  Found {len(issues)} issues")
            except Exception as e:
                error_issue = {
                    'type': 'validation_error',
                    'check': check_name,
                    'description': f'Error during {check_name}: {str(e)}'
                }
                all_issues.append(error_issue)
                print(f"  Error: {str(e)}")
        
        # Compile summary statistics
        summary = {
            'total_records': len(df),
            'total_issues': len(all_issues),
            'date_range': f"{self.start_date.date()} to {self.end_date.date()}",
            'issues_by_type': {},
            'critical_issues': [],
            'all_issues': all_issues
        }
        
        # Categorize issues
        for issue in all_issues:
            issue_type = issue.get('type', 'unknown')
            summary['issues_by_type'][issue_type] = summary['issues_by_type'].get(issue_type, 0) + 1
            
            # Flag critical issues
            critical_types = [
                'date_range_violation', 'logical_inconsistency', 
                'business_logic_violation', 'order_count_mismatch'
            ]
            if issue_type in critical_types:
                summary['critical_issues'].append(issue)
        
        return summary
    
    def print_validation_report(self, validation_results: Dict[str, Any]) -> None:
        """Print a formatted validation report."""
        print("\n" + "=" * 60)
        print("DATA QUALITY VALIDATION REPORT")
        print("=" * 60)
        
        print(f"Dataset: {validation_results['total_records']:,} records")
        print(f"Date Range: {validation_results['date_range']}")
        print(f"Total Issues Found: {validation_results['total_issues']}")
        print(f"Critical Issues: {len(validation_results['critical_issues'])}")
        
        if validation_results['total_issues'] == 0:
            print("\n✅ NO ISSUES FOUND - Data appears to be of good quality!")
            return
        
        print("\nISSUES BY TYPE:")
        for issue_type, count in validation_results['issues_by_type'].items():
            print(f"  {issue_type}: {count}")
        
        if validation_results['critical_issues']:
            print(f"\n🚨 CRITICAL ISSUES ({len(validation_results['critical_issues'])}):")
            for i, issue in enumerate(validation_results['critical_issues'], 1):
                print(f"\n{i}. {issue['type'].upper()}")
                print(f"   Description: {issue['description']}")
                if 'details' in issue:
                    print(f"   Details: {issue['details']}")
        
        print(f"\nDETAILED ISSUES ({validation_results['total_issues']}):")
        for i, issue in enumerate(validation_results['all_issues'], 1):
            print(f"\n{i}. {issue['type']}")
            print(f"   {issue['description']}")
            
            # Print additional details
            for key, value in issue.items():
                if key not in ['type', 'description']:
                    print(f"   {key}: {value}")
        
        print("\n" + "=" * 60)


def generate_sample_data() -> pd.DataFrame:
    """
    Generate sample data that mimics the expected output structure of the fulfillment care cost query.
    This is for testing purposes when actual query results are not available.
    """
    np.random.seed(42)
    n_records = 100
    
    # Generate sample data
    start_date = pd.to_datetime('2024-09-21')
    end_date = pd.to_datetime('2024-10-23')
    date_range = pd.date_range(start_date, end_date, freq='D')
    
    sample_data = {
        'cany_ind': np.random.choice(['CA', 'DCWP', 'ROM'], n_records),
        'care_cost_reason_group': np.random.choice([
            'orders with no care cost', 'logistics issues', 'restaurant issues', 'diner issues'
        ], n_records),
        'eta_care_reasons': np.random.choice(['ETA Issues', 'Other'], n_records),
        'orders': np.random.randint(1, 1000, n_records),
        'distinct_order_uuid': np.random.randint(1, 1000, n_records),
        'total_care_cost': np.random.uniform(-50, 200, n_records),
        'ghd_orders': np.random.randint(0, 1000, n_records),
        'orders_with_care_cost': np.random.randint(0, 500, n_records),
        'cancels_osmf_definition': np.random.randint(0, 100, n_records),
        'date1': np.random.choice(date_range, n_records),
        'date2': np.random.choice(date_range, n_records),
        'diner_ty_eta': np.random.uniform(-60, 180, n_records),
        'deliverytime_utc': pd.to_datetime(np.random.choice(date_range, n_records)),
        'time_local': [f"{h:02d}:{m:02d}" for h, m in zip(
            np.random.randint(0, 24, n_records),
            np.random.randint(0, 60, n_records)
        )]
    }
    
    # Ensure some records have matching orders and distinct_order_uuid
    sample_data['distinct_order_uuid'] = sample_data['orders'].copy()
    
    # Make some care cost records actually have zero cost
    zero_cost_mask = np.array([reason == 'orders with no care cost' 
                              for reason in sample_data['care_cost_reason_group']])
    sample_data['total_care_cost'][zero_cost_mask] = 0
    
    # Ensure ghd_orders <= orders
    sample_data['ghd_orders'] = np.minimum(sample_data['ghd_orders'], sample_data['orders'])
    sample_data['orders_with_care_cost'] = np.minimum(
        sample_data['orders_with_care_cost'], 
        sample_data['orders']
    )
    
    return pd.DataFrame(sample_data)


def main():
    """Main function to demonstrate the data quality validator."""
    print("Fulfillment Care Cost Data Quality Validator")
    print("=" * 50)
    
    # Example usage with sample data
    print("Generating sample data for demonstration...")
    sample_df = generate_sample_data()
    
    # Initialize validator with date range
    validator = DataQualityValidator('2024-09-21', '2024-10-23')
    
    # Run validation
    results = validator.run_validation(sample_df)
    
    # Print report
    validator.print_validation_report(results)
    
    print("\nUSAGE INSTRUCTIONS:")
    print("1. Replace sample data with actual query results:")
    print("   df = pd.read_csv('your_query_results.csv')")
    print("   # or load from your data source")
    print("")
    print("2. Initialize validator with your date parameters:")
    print("   validator = DataQualityValidator('2024-09-21', '2024-10-23')")
    print("")
    print("3. Run validation:")
    print("   results = validator.run_validation(df)")
    print("   validator.print_validation_report(results)")


if __name__ == "__main__":
    main()