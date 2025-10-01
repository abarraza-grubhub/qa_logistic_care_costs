#!/usr/bin/env python3
"""
SQL JOIN Order Analysis Tool

This script analyzes the fulfillment_care_cost.sql query to help answer:
"Ensure that large tables are specified first in a JOIN clause"

It parses the SQL query, identifies all JOIN operations, and provides recommendations
for optimizing JOIN order based on table size heuristics and query patterns.

Usage:
    python analyze_join_order.py [options]
    
Options:
    --format {text|csv|json}  Output format (default: text)
    --output FILE             Output file (default: stdout for text, auto-generated for others)
    --show-conditions         Include full JOIN conditions in output
    --verbose                 Show verbose analysis details
"""

import re
import sys
import json
import csv
import argparse
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, asdict
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
    estimated_rows: str = "Unknown"  # e.g., "10M+", "1M-10M", "<1M"


class SQLJoinAnalyzer:
    """Analyzes SQL queries for JOIN order optimization opportunities."""
    
    def __init__(self, sql_file_path: str, context_file_path: str, options: Optional[Dict] = None):
        self.sql_file_path = sql_file_path
        self.context_file_path = context_file_path
        self.joins: List[JoinClause] = []
        self.tables: Dict[str, TableInfo] = {}
        self.sql_content = ""
        self.context_content = ""
        self.table_context_info = {}  # Store information from context document
        self.options = options or {}
        
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
            estimated_rows = self._estimate_table_rows(table_name, size_category)
            
            self.tables[table_name] = TableInfo(
                name=table_name,
                full_name=full_reference,
                size_category=size_category,
                is_cte=is_cte,
                estimated_rows=estimated_rows
            )
    
    def _estimate_table_rows(self, table_name: str, size_category: str) -> str:
        """Estimate table row counts based on size category and naming patterns."""
        table_lower = table_name.lower()
        
        # Specific estimates based on table patterns
        if 'order_contribution_profit_fact' in table_lower:
            return "100M+"
        elif 'managed_delivery_fact' in table_lower:
            return "50M+"
        elif 'ticket_fact' in table_lower:
            return "10M+"
        elif 'adjustment_reporting' in table_lower or 'concession_reporting' in table_lower:
            return "5M+"
        elif 'cancellation' in table_lower and 'fact' in table_lower:
            return "1M+"
        elif 'primary_contact_reason' in table_lower or 'secondary_contact_reason' in table_lower:
            return "<1K"
        elif 'care_cost_reasons' in table_lower:
            return "<100"
        
        # General estimates based on size category
        size_to_rows = {
            'large': "1M+",
            'medium': "100K-1M",
            'small': "<100K",
            'derived': "Variable",
            'unknown': "Unknown"
        }
        
        return size_to_rows.get(size_category, "Unknown")
    
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
                'performance_impact': 'Low',
                'optimization_notes': [],
                'index_recommendations': []
            }
            
            # Analyze JOIN condition for index opportunities
            if join.join_condition and join.join_condition != "Not found":
                index_recs = self._analyze_join_condition_for_indexes(join.join_condition, join.left_table, join.right_table)
                recommendation['index_recommendations'] = index_recs
            
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
                recommendation['optimization_notes'].append("Moving larger table to left side enables more efficient hash join strategies")
            
            elif (right_table_info.size_category == 'medium' and 
                  left_table_info.size_category == 'small' and
                  not left_table_info.is_cte and not right_table_info.is_cte):
                issue_found = True
                recommendation['issue'] = f"Medium table ({join.right_table}) should typically be on the left side when joining with small table for better performance"
                recommendation['suggested_order'] = f"{join.right_table} {join.join_type} {join.left_table}"
                recommendation['priority'] = 'medium'
                recommendation['performance_impact'] = 'Medium - moderate performance improvement expected'
                recommendation['optimization_notes'].append("Reordering can improve join efficiency and reduce memory usage")
            
            elif (right_table_info.size_category == 'large' and 
                  left_table_info.size_category == 'derived'):
                recommendation['issue'] = f"Consider the size of derived table ({join.left_table}) - if small, consider moving large table ({join.right_table}) to the left"
                recommendation['suggested_order'] = f"Depends on derived table size: possibly {join.right_table} {join.join_type} {join.left_table}"
                recommendation['priority'] = 'info'
                recommendation['performance_impact'] = 'Variable - depends on derived table size'
                recommendation['optimization_notes'].append("Derived table size depends on preceding query logic - monitor actual row counts")
            
            # Add general optimization notes based on join type and table characteristics
            if join.join_type == 'LEFT JOIN' and not issue_found:
                recommendation['optimization_notes'].append("LEFT JOIN preserves all rows from left table - consider if INNER JOIN is possible for better performance")
            
            # Check for potential Cartesian product risks
            if join.join_condition == "Not found":
                existing_issue = recommendation.get('issue', '') or ''
                recommendation['issue'] = existing_issue + f" WARNING: No JOIN condition found - potential Cartesian product!"
                recommendation['priority'] = 'critical'
                recommendation['performance_impact'] = 'Critical - may cause query to hang or fail'
                recommendation['optimization_notes'].append("CRITICAL: Missing JOIN condition will create Cartesian product - query may fail or take extremely long")
            
            # Always add the recommendation (even if no issue) for completeness
            recommendations.append(recommendation)
        
        return recommendations
    
    def _analyze_join_condition_for_indexes(self, condition: str, left_table: str, right_table: str) -> List[str]:
        """Analyze JOIN condition to suggest relevant indexes."""
        index_recommendations = []
        
        # Extract column references from join condition
        # Look for patterns like table.column = other_table.column
        column_patterns = re.findall(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)', condition)
        
        for match in column_patterns:
            table1, col1, table2, col2 = match
            
            # Recommend indexes on join columns
            if table1.lower() in [left_table.lower(), right_table.lower()]:
                index_recommendations.append(f"Consider index on {table1}.{col1}")
            if table2.lower() in [left_table.lower(), right_table.lower()]:
                index_recommendations.append(f"Consider index on {table2}.{col2}")
        
        # Look for date range conditions which might benefit from partitioning
        if re.search(r'BETWEEN.*DATE', condition, re.IGNORECASE):
            index_recommendations.append("Date range condition detected - consider table partitioning by date")
        
        return list(set(index_recommendations))  # Remove duplicates
    
    def export_to_csv(self, recommendations: List[Dict], filename: str) -> None:
        """Export analysis results to CSV format."""
        if not recommendations:
            print("No data to export")
            return
            
        fieldnames = [
            'cte_name', 'line_number', 'join_type', 'left_table', 'right_table',
            'left_size', 'right_size', 'left_estimated_rows', 'right_estimated_rows',
            'current_order', 'join_condition', 'issue', 'suggested_order', 'priority', 
            'performance_impact', 'optimization_notes', 'index_recommendations'
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for rec in recommendations:
                # Add estimated rows information
                left_table_info = self.tables.get(rec['left_table'])
                right_table_info = self.tables.get(rec['right_table'])
                
                rec['left_estimated_rows'] = left_table_info.estimated_rows if left_table_info else 'Unknown'
                rec['right_estimated_rows'] = right_table_info.estimated_rows if right_table_info else 'Unknown'
                
                # Clean join condition for CSV
                if len(rec['join_condition']) > 100:
                    rec['join_condition'] = rec['join_condition'][:97] + "..."
                
                # Convert lists to strings for CSV
                rec['optimization_notes'] = '; '.join(rec.get('optimization_notes', []))
                rec['index_recommendations'] = '; '.join(rec.get('index_recommendations', []))
                
                writer.writerow({k: rec.get(k, '') for k in fieldnames})
        
        print(f"CSV export saved to: {filename}")
    
    def export_to_json(self, recommendations: List[Dict], filename: str) -> None:
        """Export analysis results to JSON format."""
        # Prepare comprehensive data structure
        export_data = {
            'metadata': {
                'total_joins': len(self.joins),
                'total_tables': len(self.tables),
                'analysis_timestamp': __import__('datetime').datetime.now().isoformat(),
                'source_files': {
                    'sql_file': self.sql_file_path,
                    'context_file': self.context_file_path
                }
            },
            'table_catalog': {
                name: asdict(info) for name, info in self.tables.items()
            },
            'join_analysis': recommendations,
            'summary': {
                'critical_issues': sum(1 for r in recommendations if r['priority'] == 'critical'),
                'high_priority': sum(1 for r in recommendations if r['priority'] == 'high'),
                'medium_priority': sum(1 for r in recommendations if r['priority'] == 'medium'),
                'info_notes': sum(1 for r in recommendations if r['priority'] == 'info')
            }
        }
        
        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(export_data, jsonfile, indent=2, ensure_ascii=False)
        
        print(f"JSON export saved to: {filename}")
    
    def generate_report(self) -> str:
        """Generate a comprehensive analysis report."""
        if not hasattr(self, 'joins') or not self.joins:
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
                    rows_info = f" - Est. rows: {table.estimated_rows}" if self.options.get('verbose') else ""
                    report.append(f"- {table.name}{cte_indicator}{rows_info}")
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
            
            # Basic information
            left_table_info = self.tables.get(rec['left_table'])
            right_table_info = self.tables.get(rec['right_table'])
            
            report.append(f"- Left table size: {rec['left_size']}")
            if left_table_info and self.options.get('verbose'):
                report.append(f"  - Estimated rows: {left_table_info.estimated_rows}")
            
            report.append(f"- Right table size: {rec['right_size']}")
            if right_table_info and self.options.get('verbose'):
                report.append(f"  - Estimated rows: {right_table_info.estimated_rows}")
            
            # JOIN condition - show full or truncated based on options
            if self.options.get('show_conditions') or len(rec['join_condition']) <= 60:
                report.append(f"- Join condition: `{rec['join_condition']}`")
            else:
                report.append(f"- Join condition: `{rec['join_condition'][:57]}...`")
            
            report.append(f"- Performance impact: {rec['performance_impact']}")
            
            # Show optimization notes if verbose
            if self.options.get('verbose') and rec.get('optimization_notes'):
                report.append("- Optimization notes:")
                for note in rec['optimization_notes']:
                    report.append(f"  • {note}")
            
            # Show index recommendations if verbose and available
            if self.options.get('verbose') and rec.get('index_recommendations'):
                report.append("- Index recommendations:")
                for idx_rec in rec['index_recommendations']:
                    report.append(f"  • {idx_rec}")
            
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
        
        # Add optimization summary if verbose
        if self.options.get('verbose'):
            report.append("## Query Optimization Summary")
            report.append("")
            
            # Collect all index recommendations
            all_index_recs = []
            for rec in recommendations:
                all_index_recs.extend(rec.get('index_recommendations', []))
            
            unique_index_recs = list(set(all_index_recs))
            if unique_index_recs:
                report.append("### Index Recommendations")
                for idx_rec in sorted(unique_index_recs):
                    report.append(f"- {idx_rec}")
                report.append("")
            
            # Performance improvement estimates
            report.append("### Estimated Performance Impact")
            report.append("- Implementing high priority recommendations: **10-50% query time reduction**")
            report.append("- Adding recommended indexes: **20-80% improvement for large JOINs**")
            report.append("- Fixing missing JOIN conditions: **Query stability and correctness**")
            report.append("")
            
            # General optimization suggestions
            report.append("### General Optimization Strategies")
            report.append("1. **Materialized Views**: Consider materializing frequently used CTEs")
            report.append("2. **Query Caching**: Enable result caching for repeated executions")
            report.append("3. **Partitioning**: Implement date-based partitioning for large fact tables")
            report.append("4. **Statistics Updates**: Ensure table statistics are current for optimal query plans")
            report.append("5. **Parallel Execution**: Enable parallel query execution for large datasets")
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
    parser = argparse.ArgumentParser(
        description="Analyze SQL JOIN order for optimization opportunities",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python analyze_join_order.py
    python analyze_join_order.py --format csv --output joins.csv
    python analyze_join_order.py --format json --verbose
    python analyze_join_order.py --show-conditions --verbose
        """
    )
    
    parser.add_argument(
        '--format',
        choices=['text', 'csv', 'json'],
        default='text',
        help='Output format (default: text)'
    )
    
    parser.add_argument(
        '--output',
        help='Output file (default: auto-generated for csv/json, stdout for text)'
    )
    
    parser.add_argument(
        '--show-conditions',
        action='store_true',
        help='Include full JOIN conditions in output'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show verbose analysis details including row estimates'
    )
    
    args = parser.parse_args()
    
    # File paths
    sql_file = "fulfillment_care_cost.sql"
    context_file = "Breaking Down Logistic Care Costs Query.md"
    
    # Check if files exist
    if not Path(sql_file).exists():
        print(f"Error: SQL file '{sql_file}' not found in current directory")
        sys.exit(1)
    
    if not Path(context_file).exists():
        print(f"Warning: Context file '{context_file}' not found in current directory")
    
    # Set up options
    options = {
        'show_conditions': args.show_conditions,
        'verbose': args.verbose
    }
    
    # Run analysis
    analyzer = SQLJoinAnalyzer(sql_file, context_file, options)
    
    try:
        analyzer.load_files()
        analyzer.parse_joins()
        recommendations = analyzer.analyze_join_order()
        
        # Generate output based on format
        if args.format == 'text':
            report = analyzer.generate_report()
            
            if args.output:
                with open(args.output, 'w', encoding='utf-8') as f:
                    f.write(report)
                print(f"Report saved to: {args.output}")
            else:
                print(report)
        
        elif args.format == 'csv':
            output_file = args.output or "join_analysis.csv"
            analyzer.export_to_csv(recommendations, output_file)
        
        elif args.format == 'json':
            output_file = args.output or "join_analysis.json"
            analyzer.export_to_json(recommendations, output_file)
        
    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()