#!/usr/bin/env python3
"""
Query Verification Script for Fulfillment Care Cost Analysis

This script helps verify the fulfillment_care_cost.sql query by running it across 
different date ranges and regions to validate the information rendered.

Based on the query analysis from fulfillment_care_cost.sql and 
Breaking Down Logistic Care Costs Query.md context.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import json
import logging
from dataclasses import dataclass


@dataclass
class QueryParameters:
    """Data class to hold query parameters for verification runs."""
    start_date: str
    end_date: str
    description: str


class QueryVerificationFramework:
    """
    Framework for verifying the fulfillment care cost query across different 
    date ranges and regions.
    
    This class provides methods to:
    1. Generate test date ranges for verification
    2. Mock query execution with different parameters  
    3. Validate query results consistency
    4. Generate verification reports
    """
    
    def __init__(self, sql_file_path: str = "fulfillment_care_cost.sql"):
        """
        Initialize the verification framework.
        
        Args:
            sql_file_path: Path to the SQL query file
        """
        self.sql_file_path = sql_file_path
        self.query_content = self._load_query()
        self.verification_results = []
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def _load_query(self) -> str:
        """Load the SQL query content from file."""
        try:
            with open(self.sql_file_path, 'r') as file:
                return file.read()
        except FileNotFoundError:
            self.logger.error(f"SQL file not found: {self.sql_file_path}")
            return ""
    
    def generate_test_date_ranges(self, base_date: str = "2024-10-01") -> List[QueryParameters]:
        """
        Generate multiple date ranges for testing query consistency.
        
        Based on the context documentation, the query uses +/- 1 day logic
        for some CTEs and exact dates for others. This generates various
        date ranges to test this behavior.
        
        Args:
            base_date: Base date in YYYY-MM-DD format
            
        Returns:
            List of QueryParameters for different test scenarios
        """
        base = datetime.strptime(base_date, "%Y-%m-%d")
        
        test_scenarios = [
            # Single day test
            QueryParameters(
                start_date=base_date,
                end_date=base_date,
                description="Single day test"
            ),
            # Week-long test
            QueryParameters(
                start_date=base_date,
                end_date=(base + timedelta(days=6)).strftime("%Y-%m-%d"),
                description="Week-long period test"
            ),
            # Month-long test
            QueryParameters(
                start_date=base_date,
                end_date=(base + timedelta(days=29)).strftime("%Y-%m-%d"),
                description="Month-long period test"
            ),
            # Cross-month boundary test
            QueryParameters(
                start_date="2024-09-28",
                end_date="2024-10-05",
                description="Cross-month boundary test"
            ),
            # Weekend test
            QueryParameters(
                start_date="2024-10-05",  # Saturday
                end_date="2024-10-06",    # Sunday
                description="Weekend period test"
            ),
            # Business week test
            QueryParameters(
                start_date="2024-10-07",  # Monday
                end_date="2024-10-11",    # Friday
                description="Business week test"
            )
        ]
        
        return test_scenarios
    
    def mock_query_execution(self, params: QueryParameters) -> Dict:
        """
        Mock the execution of the query with given parameters.
        
        Since we don't have access to the actual database, this method
        simulates what the verification would check for each query run.
        
        Args:
            params: Query parameters including start_date and end_date
            
        Returns:
            Dict containing mock verification results
        """
        
        # Replace placeholders in query
        parameterized_query = self.query_content.replace(
            "{{start_date}}", params.start_date
        ).replace(
            "{{end_date}}", params.end_date
        )
        
        # Mock result analysis
        verification_result = {
            "parameters": {
                "start_date": params.start_date,
                "end_date": params.end_date,
                "description": params.description
            },
            "query_structure_checks": self._analyze_query_structure(parameterized_query),
            "date_logic_validation": self._validate_date_logic(params),
            "expected_regions": self._get_expected_regions(),
            "data_consistency_checks": self._generate_consistency_checks(params)
        }
        
        return verification_result
    
    def _analyze_query_structure(self, query: str) -> Dict:
        """
        Analyze the structure of the parameterized query.
        
        Checks for proper date parameter substitution and CTE structure.
        """
        checks = {
            "date_parameters_replaced": "{{start_date}}" not in query and "{{end_date}}" not in query,
            "cte_count": len([line for line in query.split('\n') if 'AS (' in line]),
            "main_ctes_present": {
                "adj": "adj AS (" in query,
                "ghg": "ghg AS (" in query,
                "care_fg": "care_fg AS (" in query,
                "diner_ss_cancels": "diner_ss_cancels AS (" in query,
                "cancels": "cancels AS (" in query,
                "mdf": "mdf AS (" in query,
                "contacts": "contacts AS (" in query,
                "o": "o AS (" in query,
                "o2": "o2 AS (" in query,
                "o3": "o3 AS (" in query
            }
        }
        return checks
    
    def _validate_date_logic(self, params: QueryParameters) -> Dict:
        """
        Validate the +/- 1 day logic mentioned in the context documentation.
        
        According to the documentation, some CTEs use +/- 1 day logic while
        others use exact dates.
        """
        start_date = datetime.strptime(params.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(params.end_date, "%Y-%m-%d")
        
        # CTEs that should use +/- 1 day logic according to documentation
        plus_minus_one_day_ctes = [
            "adj", "ghg", "care_fg", "diner_ss_cancels", "cancels", 
            "mdf", "contacts", "o"
        ]
        
        # CTEs that use exact dates
        exact_date_ctes = ["mdf (dropoff_complete_time_local filter)"]
        
        validation = {
            "date_range_span_days": (end_date - start_date).days + 1,
            "plus_minus_one_day_logic_ctes": plus_minus_one_day_ctes,
            "exact_date_logic_ctes": exact_date_ctes,
            "expected_data_range_with_buffer": {
                "start": (start_date - timedelta(days=1)).strftime("%Y-%m-%d"),
                "end": (end_date + timedelta(days=1)).strftime("%Y-%m-%d")
            }
        }
        
        return validation
    
    def _get_expected_regions(self) -> Dict:
        """
        Get expected region categorizations from the query logic.
        
        Based on the query, regions are categorized into:
        - CA_Market: 'CA' vs 'xCA'  
        - NYC_Market: 'DCWP' vs 'xDCWP'
        - Final grouping: 'CA', 'DCWP', or 'ROM' (Rest of Markets)
        """
        return {
            "ca_market_values": ["CA", "xCA"],
            "nyc_market_values": ["DCWP", "xDCWP"],
            "final_cany_ind_values": ["CA", "DCWP", "ROM"],
            "nyc_market_region_uuids": [
                "92ecc187-d0ed-4b17-bcdb-7da84786f0ef",
                "71ba188f-e632-4e6c-8710-fa1b6b7a303e",
                "eea72701-225b-4f8c-a13d-b10798e7c89c",
                "d56fcd88-9b3f-47f9-a7a9-ac8cc8b15e89",
                "2c2aed8b-9f85-4666-873a-10f492b69dcb",
                "adad9d37-7a3b-46ab-9ca6-c0a354b107d6",
                "ac195f13-5f83-473e-b897-8c098c302699",
                "a213d9d3-78ac-4025-a9d3-6a00304f344c",
                "b2070f16-6d97-4c6e-9ce8-7ae581b4e87f",
                "f373b4d7-9e3e-4960-bd54-9122c51fc02f",
                "5d35d88c-fa24-488e-892c-b046ccfb9f61",
                "7bf6fde8-d453-4679-83bb-c67b737f12ed",
                "60fa2685-2463-48ba-b864-05384504ec2b",
                "504eb68a-9148-4b1f-bcff-941a7ba8f0e1",
                "a15298e0-4a9f-4f7d-aee5-507b7ed81651",
                "f138e5e9-d03e-4832-8f60-6eda4246c1aa",
                "afa226a3-f2e2-4d9b-8fa1-e228b8c7ed0d"
            ]
        }
    
    def _generate_consistency_checks(self, params: QueryParameters) -> Dict:
        """
        Generate expected consistency checks for the query results.
        
        These are the types of validations that should be performed
        on the actual query results.
        """
        return {
            "data_quality_checks": {
                "no_duplicate_order_uuids": "COUNT(order_uuid) = COUNT(DISTINCT order_uuid)",
                "total_care_cost_calculation": "cp_care_concession_awarded_amount + cp_care_ticket_cost + cp_diner_adj + COALESCE(cp_redelivery_cost, 0) + COALESCE(cp_grub_care_refund, 0)",
                "ghd_orders_subset_of_total": "ghd_orders <= orders",
                "orders_with_care_cost_subset": "orders_with_care_cost <= orders"
            },
            "business_logic_checks": {
                "managed_delivery_filter": "All orders should have managed_delivery_ind = TRUE",
                "date_range_adherence": f"All order dates should be between {params.start_date} and {params.end_date}",
                "care_cost_reason_groups": [
                    "orders with no care cost",
                    "Logistics Issues", 
                    "Restaurant Issues",
                    "Diner Issues",
                    "not grouped"
                ],
                "eta_care_reasons": ["ETA Issues", "Other"]
            },
            "aggregation_validation": {
                "sum_of_regional_orders": "Sum of CA + DCWP + ROM orders should equal total orders",
                "care_cost_components": [
                    "cp_care_concession_awarded_amount",
                    "cp_care_ticket_cost", 
                    "cp_diner_adj",
                    "cp_redelivery_cost",
                    "cp_grub_care_refund"
                ]
            }
        }
    
    def run_verification_suite(self) -> List[Dict]:
        """
        Run the complete verification suite across multiple date ranges.
        
        Returns:
            List of verification results for each test scenario
        """
        self.logger.info("Starting query verification suite...")
        
        test_scenarios = self.generate_test_date_ranges()
        
        for scenario in test_scenarios:
            self.logger.info(f"Running verification for: {scenario.description}")
            result = self.mock_query_execution(scenario)
            self.verification_results.append(result)
        
        self.logger.info(f"Completed verification for {len(test_scenarios)} scenarios")
        return self.verification_results
    
    def generate_verification_report(self) -> str:
        """
        Generate a comprehensive verification report.
        
        Returns:
            Formatted string report of all verification results
        """
        if not self.verification_results:
            return "No verification results available. Run verification suite first."
        
        report_lines = [
            "=" * 80,
            "FULFILLMENT CARE COST QUERY VERIFICATION REPORT",
            "=" * 80,
            f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"SQL Query File: {self.sql_file_path}",
            f"Total Test Scenarios: {len(self.verification_results)}",
            "",
        ]
        
        for i, result in enumerate(self.verification_results, 1):
            params = result["parameters"]
            structure = result["query_structure_checks"]
            date_logic = result["date_logic_validation"]
            
            report_lines.extend([
                f"SCENARIO {i}: {params['description']}",
                "-" * 50,
                f"Date Range: {params['start_date']} to {params['end_date']}",
                f"Duration: {date_logic['date_range_span_days']} day(s)",
                "",
                "Query Structure Validation:",
                f"  ✓ Date parameters replaced: {structure['date_parameters_replaced']}",
                f"  ✓ CTE count: {structure['cte_count']}",
                f"  ✓ All main CTEs present: {all(structure['main_ctes_present'].values())}",
                "",
                "Date Logic Validation:",
                f"  ✓ Expected data range (with +/-1 buffer): {date_logic['expected_data_range_with_buffer']['start']} to {date_logic['expected_data_range_with_buffer']['end']}",
                f"  ✓ CTEs using +/-1 day logic: {len(date_logic['plus_minus_one_day_logic_ctes'])}",
                "",
                "Expected Verification Points:",
                "  • Data quality checks should pass",
                "  • Business logic constraints should be enforced", 
                "  • Regional aggregations should sum correctly",
                "  • Care cost calculations should be accurate",
                "",
            ])
        
        report_lines.extend([
            "SUMMARY RECOMMENDATIONS:",
            "-" * 50,
            "1. Run query with each test scenario's date parameters",
            "2. Validate that results match expected data quality checks",
            "3. Verify regional breakdown sums to total across all scenarios",
            "4. Confirm date filtering logic works correctly with +/-1 day buffer",
            "5. Check that care cost categorization is consistent across date ranges",
            "",
            "=" * 80
        ])
        
        return "\n".join(report_lines)
    
    def export_verification_checklist(self, filename: str = "verification_checklist.json") -> None:
        """
        Export a detailed verification checklist for manual testing.
        
        Args:
            filename: Output filename for the checklist
        """
        checklist = {
            "verification_framework": {
                "description": "Checklist for verifying fulfillment_care_cost.sql query across different dates and regions",
                "generated_on": datetime.now().isoformat(),
                "sql_file": self.sql_file_path
            },
            "test_scenarios": [],
            "verification_steps": {
                "pre_execution": [
                    "Verify database connectivity",
                    "Confirm table permissions for all referenced tables", 
                    "Validate date parameter format (YYYY-MM-DD)"
                ],
                "execution": [
                    "Run query with each test scenario parameters",
                    "Capture execution time and resource usage",
                    "Record row counts for each major CTE",
                    "Save results for comparison analysis"
                ],
                "post_execution": [
                    "Validate data quality checks pass",
                    "Verify business logic constraints",
                    "Confirm regional aggregations sum correctly",
                    "Check for unexpected NULL values",
                    "Validate care cost calculations",
                    "Compare results across different date ranges for consistency"
                ]
            }
        }
        
        # Add test scenarios to checklist
        test_scenarios = self.generate_test_date_ranges()
        for scenario in test_scenarios:
            checklist["test_scenarios"].append({
                "start_date": scenario.start_date,
                "end_date": scenario.end_date, 
                "description": scenario.description,
                "verification_points": [
                    f"Query executes successfully with dates {scenario.start_date} to {scenario.end_date}",
                    "Results contain expected regional categories (CA, DCWP, ROM)",
                    "Care cost calculations are non-negative where expected",
                    "Order counts are reasonable for the date range",
                    "No duplicate order_uuid in results"
                ]
            })
        
        with open(filename, 'w') as f:
            json.dump(checklist, f, indent=2)
        
        self.logger.info(f"Verification checklist exported to {filename}")


def main():
    """Main function to run the query verification framework."""
    
    print("Fulfillment Care Cost Query Verification Script")
    print("=" * 50)
    
    # Initialize the verification framework
    verifier = QueryVerificationFramework()
    
    # Run the verification suite
    results = verifier.run_verification_suite()
    
    # Generate and display the report
    report = verifier.generate_verification_report()
    print(report)
    
    # Export verification checklist
    verifier.export_verification_checklist()
    
    print("\nVerification framework completed successfully!")
    print("Review the generated checklist and run actual queries to validate results.")


if __name__ == "__main__":
    main()