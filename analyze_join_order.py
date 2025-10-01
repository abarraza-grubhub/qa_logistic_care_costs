#!/usr/bin/env python3
"""
SQL JOIN Order Analysis Tool

This script analyzes the fulfillment_care_cost.sql query to help answer:
"Ensure that large tables are specified first in a JOIN clause"

It parses the SQL query, identifies all JOIN operations, and provides recommendations
for optimizing JOIN order based on table size heuristics and query patterns.
"""

import re
import sys
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from pathlib import Path


@dataclass
class JoinClause:
    """Represents a JOIN clause in the SQL query."""
    join_type: str  # e.g., 'LEFT JOIN', 'JOIN', 'INNER JOIN'
    left_table: str
    right_table: str
    join_condition: str
    cte_name: Optional[str] = None
    line_number: int = 0


@dataclass
class TableInfo:
    """Information about a table including estimated size category."""
    name: str
    full_name: str
    size_category: str  # 'large', 'medium', 'small', 'unknown'
    is_cte: bool = False
    description: str = ""


class SQLJoinAnalyzer:
    """Analyzes SQL queries for JOIN order optimization opportunities."""
    
    def __init__(self, sql_file_path: str, context_file_path: str):
        self.sql_file_path = sql_file_path
        self.context_file_path = context_file_path
        self.joins: List[JoinClause] = []
        self.tables: Dict[str, TableInfo] = {}
        self.sql_content = ""
        self.context_content = ""
        self.table_context_info = {}  # Store information from context document
        
    def load_files(self) -> None:
        """Load SQL and context files."""
        try:
            with open(self.sql_file_path, 'r', encoding='utf-8') as f:
                self.sql_content = f.read()
        except FileNotFoundError:
            print(f"Error: SQL file not found: {self.sql_file_path}")
            sys.exit(1)
            
        try:
            with open(self.context_file_path, 'r', encoding='utf-8') as f:
                self.context_content = f.read()
            self._parse_context_info()
        except FileNotFoundError:
            print(f"Warning: Context file not found: {self.context_file_path}")
            self.context_content = ""
    
    def _parse_context_info(self) -> None:
        """Extract table information from the context document."""
        if not self.context_content:
            return
        
        # Look for table descriptions in the context
        table_info_patterns = [
            # Look for table descriptions in the data sources section
            (r'(\w+\.[\w_]+)\s+.*Contains.*', 'description'),
            (r'(\w+\.[\w_]+)\s+.*fact table.*', 'large'),
            (r'(\w+\.[\w_]+)\s+.*reference table.*', 'small'),
            (r'(\w+\.[\w_]+)\s+.*dimension.*', 'medium'),
            (r'(\w+\.[\w_]+)\s+.*lookup.*', 'small'),
        ]
        
        for pattern, info_type in table_info_patterns:
            matches = re.findall(pattern, self.context_content, re.IGNORECASE)
            for match in matches:
                table_name = self.extract_table_name(match)
                if table_name not in self.table_context_info:
                    self.table_context_info[table_name] = {}
                if info_type in ['large', 'medium', 'small']:
                    self.table_context_info[table_name]['size_hint'] = info_type
                elif info_type == 'description':
                    self.table_context_info[table_name]['has_description'] = True
    
    def categorize_table_size(self, table_name: str) -> str:
        """
        Categorize table size based on naming conventions and context information.
        
        This is a heuristic approach based on:
        1. Context document information
        2. Table naming patterns
        3. Common data warehouse patterns
        """
        table_lower = table_name.lower()
        
        # First check if we have context information about this table
        if table_name in self.table_context_info:
            size_hint = self.table_context_info[table_name].get('size_hint')
            if size_hint:
                return size_hint
        
        # Large tables (fact tables, operational data)
        large_table_patterns = [
            'fact',
            'order_contribution_profit_fact',
            'managed_delivery_fact_v2',
            'ticket_fact',
            'order_cancellation_fact',
            'adjustment_reporting',
            'concession_reporting',
            'guarantee_claim',
            'cancellation_result'
        ]
        
        # Medium tables (dimension tables, reference data)
        medium_table_patterns = [
            'cancellation_reason_map'
        ]
        
        # Small tables (reference/lookup tables)
        small_table_patterns = [
            'primary_contact_reason',
            'secondary_contact_reason',
            'care_cost_reasons'
        ]
        
        for pattern in large_table_patterns:
            if pattern in table_lower:
                return 'large'
                
        for pattern in medium_table_patterns:
            if pattern in table_lower:
                return 'medium'
                
        for pattern in small_table_patterns:
            if pattern in table_lower:
                return 'small'
        
        # Default categorization based on schema patterns
        if 'integrated_' in table_lower and 'fact' in table_lower:
            return 'large'  # Integrated fact tables are typically large
        elif 'integrated_' in table_lower:
            return 'medium'  # Other integrated tables are medium
        elif 'source_' in table_lower and ('reporting' in table_lower or 'fact' in table_lower):
            return 'large'  # Source reporting/fact tables are large
        elif 'source_' in table_lower:
            return 'medium'  # Other source tables are medium
        elif 'ref' in table_lower or 'reference' in table_lower:
            return 'small'  # Reference tables are typically small
        elif 'ods.' in table_lower:
            return 'large'  # ODS tables are usually large
        elif table_lower in ['adj', 'ghg', 'care_fg', 'diner_ss_cancels', 'cancels', 'mdf', 'contacts', 'o', 'o2', 'o3']:
            return 'derived'  # CTEs - size depends on their source data
            
        return 'unknown'
    
    def extract_table_name(self, table_reference: str) -> str:
        """Extract clean table name from various formats."""
        # Remove schema prefixes and aliases
        table_reference = table_reference.strip()
        
        # Handle aliases (e.g., "table_name alias" or "table_name AS alias")
        parts = re.split(r'\s+(?:as\s+)?', table_reference.lower())
        table_part = parts[0]
        
        # Extract just the table name without schema
        if '.' in table_part:
            return table_part.split('.')[-1]
        
        return table_part
    
    def parse_joins(self) -> None:
        """Parse all JOIN clauses from the SQL query."""
        lines = self.sql_content.split('\n')
        current_cte = None
        
        # Pattern to match CTE definitions
        cte_pattern = re.compile(r'^\s*(\w+)\s+AS\s*\(', re.IGNORECASE)
        
        # Pattern to match JOIN clauses (JOIN keyword on its own line or with table)
        join_pattern = re.compile(r'^\s*((?:LEFT\s+|RIGHT\s+|INNER\s+|FULL\s+|CROSS\s+)?JOIN)\s*(.*)$', re.IGNORECASE)
        
        for i, line in enumerate(lines, 1):
            # Check for CTE definition
            cte_match = cte_pattern.search(line)
            if cte_match:
                current_cte = cte_match.group(1)
                continue
            
            # Check for JOIN clause
            join_match = join_pattern.search(line)
            if join_match:
                join_type = join_match.group(1).strip().upper()
                right_table_part = join_match.group(2).strip()
                
                # Handle case where table is on the next line
                table_line_idx = i
                if not right_table_part and i < len(lines):
                    next_line = lines[i].strip()
                    if next_line and not next_line.startswith('ON'):
                        right_table_part = next_line
                        table_line_idx = i + 1
                
                if not right_table_part:
                    continue
                    
                right_table_raw = right_table_part
                right_table = self.extract_table_name(right_table_raw)
                
                # Find the left table by looking at the preceding FROM or JOIN
                left_table = self._find_left_table(lines, i)
                
                # Extract join condition (ON clause) - start looking from the table line
                join_condition = self._extract_join_condition(lines, table_line_idx)
                
                join_clause = JoinClause(
                    join_type=join_type,
                    left_table=left_table,
                    right_table=right_table,
                    join_condition=join_condition,
                    cte_name=current_cte,
                    line_number=i
                )
                
                self.joins.append(join_clause)
                
                # Register tables
                self._register_table(left_table, left_table)
                self._register_table(right_table, right_table_raw)
    
    def _find_left_table(self, lines: List[str], join_line_idx: int) -> str:
        """Find the left table for a JOIN by looking backwards."""
        # Look backwards for FROM clause or previous JOIN
        for i in range(join_line_idx - 2, -1, -1):
            line = lines[i].strip()
            
            # Skip empty lines and comments
            if not line or line.startswith('--'):
                continue
            
            # Check for FROM clause on its own line
            if re.search(r'^\s*FROM\s*$', line, re.IGNORECASE):
                # FROM is on its own line, table should be on next line
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if next_line and not next_line.startswith('--'):
                        return self.extract_table_name(next_line)
            
            # Check for FROM clause with table name
            from_match = re.search(r'FROM\s+([^\s,\)]+)', line, re.IGNORECASE)
            if from_match:
                table_ref = from_match.group(1).strip()
                return self.extract_table_name(table_ref)
            
            # Check for previous JOIN with table on same line
            join_match = re.search(r'(?:LEFT\s+|RIGHT\s+|INNER\s+|FULL\s+|CROSS\s+)?JOIN\s+([^\s,\)]+)', line, re.IGNORECASE)
            if join_match:
                table_ref = join_match.group(1).strip()
                return self.extract_table_name(table_ref)
                
            # Check if this line is just a table name (after a JOIN on previous line or after FROM)
            table_line_match = re.match(r'^\s*([a-zA-Z_][a-zA-Z0-9_.]*)\s*(?:[a-zA-Z_][a-zA-Z0-9_]*\s*)?$', line)
            if table_line_match and i > 0:
                # Look at previous line to see if it was a JOIN or FROM
                prev_line = lines[i-1].strip()
                if re.search(r'(?:LEFT\s+|RIGHT\s+|INNER\s+|FULL\s+|CROSS\s+)?JOIN\s*$', prev_line, re.IGNORECASE):
                    return self.extract_table_name(table_line_match.group(1))
                elif re.search(r'FROM\s*$', prev_line, re.IGNORECASE):
                    return self.extract_table_name(table_line_match.group(1))
            
            # Stop searching if we hit a SELECT, WITH, or closing parenthesis
            if re.search(r'^\s*(SELECT|WITH|\))', line, re.IGNORECASE):
                break
        
        return "unknown"
    
    def _extract_join_condition(self, lines: List[str], start_idx: int) -> str:
        """Extract the ON condition for a JOIN."""
        condition_lines = []
        
        # Look for ON clause starting from the given position
        for i in range(start_idx, min(len(lines), start_idx + 8)):
            line = lines[i].strip()
            
            # Skip empty lines and comments
            if not line or line.startswith('--'):
                continue
            
            # Found ON clause
            if re.search(r'^\s*ON\b', line, re.IGNORECASE):
                # Extract the condition part
                on_match = re.search(r'ON\s+(.+)', line, re.IGNORECASE)
                if on_match:
                    condition_lines.append(on_match.group(1).strip())
                
                # Continue collecting multi-line conditions
                for j in range(i + 1, min(len(lines), i + 5)):
                    next_line = lines[j].strip()
                    
                    # Skip comments and empty lines
                    if not next_line or next_line.startswith('--'):
                        continue
                    
                    # Check if this line is a continuation of the condition
                    if re.search(r'^\s*AND\s+', next_line, re.IGNORECASE):
                        condition_lines.append(next_line)
                    elif re.search(r'^\s*(LEFT\s+JOIN|RIGHT\s+JOIN|INNER\s+JOIN|FULL\s+JOIN|JOIN|FROM|WHERE|GROUP|ORDER|SELECT|\))', next_line, re.IGNORECASE):
                        # Stop if we hit a new clause
                        break
                    # If it's not a clear stop word and doesn't start with AND, it might still be part of the condition
                    # But let's be conservative and stop here
                    else:
                        break
                
                break
            
            # Stop if we hit another major clause without finding ON
            elif re.search(r'^\s*(LEFT\s+JOIN|RIGHT\s+JOIN|INNER\s+JOIN|FULL\s+JOIN|JOIN|FROM|WHERE|GROUP|ORDER|SELECT)', line, re.IGNORECASE):
                break
        
        condition_text = ' '.join(condition_lines).strip()
        
        # Clean up the condition text
        if condition_text:
            # Remove extra whitespace
            condition_text = re.sub(r'\s+', ' ', condition_text)
            # Truncate if too long for display
            if len(condition_text) > 120:
                condition_text = condition_text[:117] + "..."
        
        return condition_text or "Not found"
    
    def _register_table(self, table_name: str, full_reference: str) -> None:
        """Register a table with its metadata."""
        if table_name not in self.tables:
            size_category = self.categorize_table_size(full_reference)
            is_cte = not ('.' in full_reference)  # CTEs typically don't have schema prefixes
            
            self.tables[table_name] = TableInfo(
                name=table_name,
                full_name=full_reference,
                size_category=size_category,
                is_cte=is_cte
            )
    
    def analyze_join_order(self) -> List[Dict]:
        """Analyze JOIN order and identify optimization opportunities."""
        recommendations = []
        
        for join in self.joins:
            left_table_info = self.tables.get(join.left_table)
            right_table_info = self.tables.get(join.right_table)
            
            if not left_table_info or not right_table_info:
                continue
            
            # Check if large table should be first
            issue_found = False
            recommendation = {
                'cte_name': join.cte_name,
                'line_number': join.line_number,
                'join_type': join.join_type,
                'left_table': join.left_table,
                'right_table': join.right_table,
                'left_size': left_table_info.size_category,
                'right_size': right_table_info.size_category,
                'current_order': f"{join.left_table} {join.join_type} {join.right_table}",
                'join_condition': join.join_condition,
                'issue': None,
                'suggested_order': None,
                'priority': 'low',
                'performance_impact': 'Low'
            }
            
            # Check for size-based optimization opportunities
            # Only flag issues when we have clear size differences with physical tables
            if (right_table_info.size_category == 'large' and 
                left_table_info.size_category in ['medium', 'small'] and
                not left_table_info.is_cte and not right_table_info.is_cte):
                issue_found = True
                recommendation['issue'] = f"Large table ({join.right_table}) should typically be on the left side of JOIN for better performance"
                recommendation['suggested_order'] = f"{join.right_table} {join.join_type} {join.left_table}"
                recommendation['priority'] = 'high'
                recommendation['performance_impact'] = 'High - can significantly reduce query execution time'
            
            elif (right_table_info.size_category == 'medium' and 
                  left_table_info.size_category == 'small' and
                  not left_table_info.is_cte and not right_table_info.is_cte):
                issue_found = True
                recommendation['issue'] = f"Medium table ({join.right_table}) should typically be on the left side when joining with small table for better performance"
                recommendation['suggested_order'] = f"{join.right_table} {join.join_type} {join.left_table}"
                recommendation['priority'] = 'medium'
                recommendation['performance_impact'] = 'Medium - moderate performance improvement expected'
            
            elif (right_table_info.size_category == 'large' and 
                  left_table_info.size_category == 'derived'):
                recommendation['issue'] = f"Consider the size of derived table ({join.left_table}) - if small, consider moving large table ({join.right_table}) to the left"
                recommendation['suggested_order'] = f"Depends on derived table size: possibly {join.right_table} {join.join_type} {join.left_table}"
                recommendation['priority'] = 'info'
                recommendation['performance_impact'] = 'Variable - depends on derived table size'
            
            # Check for potential Cartesian product risks
            if join.join_condition == "Not found":
                existing_issue = recommendation.get('issue', '') or ''
                recommendation['issue'] = existing_issue + f" WARNING: No JOIN condition found - potential Cartesian product!"
                recommendation['priority'] = 'critical'
                recommendation['performance_impact'] = 'Critical - may cause query to hang or fail'
            
            # Always add the recommendation (even if no issue) for completeness
            recommendations.append(recommendation)
        
        return recommendations
    
    def generate_report(self) -> str:
        """Generate a comprehensive analysis report."""
        self.load_files()
        self.parse_joins()
        recommendations = self.analyze_join_order()
        
        report = []
        report.append("# SQL JOIN Order Analysis Report")
        report.append("=" * 50)
        report.append("")
        report.append("## Summary")
        report.append(f"- Total JOINs analyzed: {len(self.joins)}")
        report.append(f"- Tables identified: {len(self.tables)}")
        
        # Count issues by priority
        high_priority = sum(1 for r in recommendations if r['priority'] == 'high')
        medium_priority = sum(1 for r in recommendations if r['priority'] == 'medium')
        info_priority = sum(1 for r in recommendations if r['priority'] == 'info')
        critical_priority = sum(1 for r in recommendations if r['priority'] == 'critical')
        
        report.append(f"- Critical issues: {critical_priority}")
        report.append(f"- High priority optimization opportunities: {high_priority}")
        report.append(f"- Medium priority optimization opportunities: {medium_priority}")
        report.append(f"- Informational notes: {info_priority}")
        report.append("")
        
        # Table size summary
        report.append("## Table Size Categories")
        report.append("")
        for size in ['large', 'medium', 'small', 'derived', 'unknown']:
            tables_in_category = [t for t in self.tables.values() if t.size_category == size]
            if tables_in_category:
                report.append(f"### {size.title()} Tables:")
                for table in sorted(tables_in_category, key=lambda x: x.name):
                    cte_indicator = " (CTE)" if table.is_cte else ""
                    report.append(f"- {table.name}{cte_indicator}")
                report.append("")
        
        # JOIN analysis
        report.append("## JOIN Analysis Details")
        report.append("")
        
        current_cte = None
        for rec in recommendations:
            if rec['cte_name'] != current_cte:
                current_cte = rec['cte_name']
                report.append(f"### CTE: {current_cte or 'Main Query'}")
                report.append("")
            
            report.append(f"**Line {rec['line_number']}:** `{rec['current_order']}`")
            report.append(f"- Left table size: {rec['left_size']}")
            report.append(f"- Right table size: {rec['right_size']}")
            report.append(f"- Join condition: `{rec['join_condition']}`")
            report.append(f"- Performance impact: {rec['performance_impact']}")
            
            if rec['issue']:
                if rec['priority'] == 'info':
                    report.append(f"- ℹ️  **INFO**: {rec['issue']}")
                    report.append(f"- 💡 **Suggestion**: {rec['suggested_order']}")
                elif rec['priority'] == 'critical':
                    report.append(f"- 🚨 **CRITICAL**: {rec['issue']}")
                    if rec['suggested_order']:
                        report.append(f"- 💡 **Suggested**: `{rec['suggested_order']}`")
                else:
                    report.append(f"- ⚠️  **{rec['priority'].upper()} PRIORITY**: {rec['issue']}")
                    report.append(f"- 💡 **Suggested**: `{rec['suggested_order']}`")
            else:
                report.append("- ✅ **Status**: JOIN order appears optimal")
            
            report.append("")
        
        # Recommendations summary
        if critical_priority > 0 or high_priority > 0 or medium_priority > 0 or info_priority > 0:
            report.append("## Key Recommendations")
            report.append("")
            
            if critical_priority > 0:
                report.append("### 🚨 Critical Issues (Immediate Action Required)")
                for rec in recommendations:
                    if rec['priority'] == 'critical':
                        report.append(f"- **Line {rec['line_number']}** in CTE '{rec['cte_name']}': {rec['issue']}")
                report.append("")
            
            if high_priority > 0:
                report.append("### ⚠️ High Priority Issues")
                for rec in recommendations:
                    if rec['priority'] == 'high':
                        report.append(f"- **Line {rec['line_number']}** in CTE '{rec['cte_name']}': {rec['issue']}")
                report.append("")
            
            if medium_priority > 0:
                report.append("### Medium Priority Issues")
                for rec in recommendations:
                    if rec['priority'] == 'medium':
                        report.append(f"- **Line {rec['line_number']}** in CTE '{rec['cte_name']}': {rec['issue']}")
                report.append("")
                
            if info_priority > 0:
                report.append("### Informational Notes")
                for rec in recommendations:
                    if rec['priority'] == 'info':
                        report.append(f"- **Line {rec['line_number']}** in CTE '{rec['cte_name']}': {rec['issue']}")
                report.append("")
        
        # Additional notes
        report.append("## Notes")
        report.append("")
        report.append("- This analysis is based on heuristics derived from table naming conventions")
        report.append("- Actual table sizes may vary and should be verified with database statistics")
        report.append("- JOIN order optimization impact depends on the query optimizer and database system")
        report.append("- Consider creating database statistics or using EXPLAIN PLAN for definitive optimization")
        
        return "\n".join(report)


def main():
    """Main function to run the JOIN analysis."""
    # File paths
    sql_file = "fulfillment_care_cost.sql"
    context_file = "Breaking Down Logistic Care Costs Query.md"
    
    # Check if files exist
    if not Path(sql_file).exists():
        print(f"Error: SQL file '{sql_file}' not found in current directory")
        sys.exit(1)
    
    if not Path(context_file).exists():
        print(f"Warning: Context file '{context_file}' not found in current directory")
    
    # Run analysis
    analyzer = SQLJoinAnalyzer(sql_file, context_file)
    
    try:
        report = analyzer.generate_report()
        
        # Output report
        print(report)
        
        # Also save to file
        output_file = "join_order_analysis_report.md"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(f"\n\nReport saved to: {output_file}")
        
    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()