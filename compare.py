#!/usr/bin/env python3
"""
Snowflake Data Comparison Framework for Pipeline Validation

This tool compares data between old (SSIS) and new (Airflow/dbt) Snowflake schemas
using row-by-row comparison to validate pipeline migration.
"""

import os
import sys
import logging
import json
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import click
import yaml
from data_diff import connect_to_table, diff_tables
from tabulate import tabulate
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


class SnowflakeDataComparer:
    def __init__(self, config_path: str, output_dir: str = "outputs", log_dir: str = "logs"):
        self.config = self._load_config(config_path)
        self.output_dir = Path(output_dir)
        self.log_dir = Path(log_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.log_dir.mkdir(exist_ok=True)
        
        self.results = []
        self.summary = {
            "total_tables": 0,
            "passed_tables": 0,
            "failed_tables": 0,
            "errors": []
        }
        
        self._setup_logging()
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            raise ValueError(f"Failed to load config from {config_path}: {str(e)}")
    
    def _setup_logging(self):
        """Setup logging to file and console"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.log_dir / f"comparison_{timestamp}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Starting data comparison - Log file: {log_file}")
    
    def _build_connection_string(self, env_type: str) -> str:
        """Build Snowflake connection string for legacy or new environment"""
        if env_type == "legacy":
            sf_config = self.config['legacy_snowflake']
        elif env_type == "new":
            sf_config = self.config['new_snowflake']
        else:
            raise ValueError(f"Invalid environment type: {env_type}")
            
        return (
            f"snowflake://{sf_config['user']}:{sf_config['password']}"
            f"@{sf_config['account']}/{sf_config['database']}/{sf_config['schema']}"
            f"?warehouse={sf_config['warehouse']}&role={sf_config['role']}"
        )
    
    def compare_table(self, table_config: Dict[str, Any]) -> Dict[str, Any]:
        """Compare a single table between legacy and new environments"""
        table_name = table_config['name']
        keys = table_config['keys']
        
        self.logger.info(f"Comparing table: {table_name}")
        
        try:
            # Build connection strings for both environments
            legacy_conn = self._build_connection_string("legacy")
            new_conn = self._build_connection_string("new")
            
            # Connect to tables
            legacy_table = connect_to_table(
                legacy_conn, 
                self.config['legacy_snowflake']['schema'], 
                table_name
            )
            new_table = connect_to_table(
                new_conn, 
                self.config['new_snowflake']['schema'], 
                table_name
            )
            
            # Perform comparison
            start_time = datetime.now()
            diffs = list(diff_tables(legacy_table, new_table, on=keys))
            end_time = datetime.now()
            
            # Process results
            added_rows = [d for d in diffs if str(d).startswith('+')]
            removed_rows = [d for d in diffs if str(d).startswith('-')]
            
            result = {
                "table": table_name,
                "status": "PASS" if len(diffs) == 0 else "FAIL",
                "total_diffs": len(diffs),
                "added_rows": len(added_rows),
                "removed_rows": len(removed_rows),
                "execution_time": str(end_time - start_time),
                "timestamp": datetime.now().isoformat(),
                "diffs": [str(d) for d in diffs[:self.config.get('comparison', {}).get('max_diffs', 1000)]],
                "legacy_environment": self.config['legacy_snowflake']['account'],
                "new_environment": self.config['new_snowflake']['account']
            }
            
            if result["status"] == "PASS":
                self.logger.info(f"✓ {table_name}: PASSED - No differences found")
                self.summary["passed_tables"] += 1
            else:
                self.logger.warning(f"✗ {table_name}: FAILED - {len(diffs)} differences found")
                self.summary["failed_tables"] += 1
                
            return result
            
        except Exception as e:
            error_msg = f"Error comparing {table_name}: {str(e)}"
            self.logger.error(error_msg)
            self.summary["errors"].append(error_msg)
            
            return {
                "table": table_name,
                "status": "ERROR",
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
                "legacy_environment": self.config.get('legacy_snowflake', {}).get('account', 'unknown'),
                "new_environment": self.config.get('new_snowflake', {}).get('account', 'unknown')
            }
    
    def compare_all_tables(self) -> List[Dict[str, Any]]:
        """Compare all tables defined in configuration"""
        self.summary["total_tables"] = len(self.config['tables'])
        
        for table_config in self.config['tables']:
            result = self.compare_table(table_config)
            self.results.append(result)
        
        return self.results
    
    def generate_summary_report(self) -> str:
        """Generate summary report"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Create summary table
        summary_data = [
            ["Total Tables", self.summary["total_tables"]],
            ["Passed Tables", self.summary["passed_tables"]],
            ["Failed Tables", self.summary["failed_tables"]],
            ["Success Rate", f"{(self.summary['passed_tables']/self.summary['total_tables']*100):.1f}%" if self.summary['total_tables'] > 0 else "0%"]
        ]
        
        report = f"""
# Snowflake Data Comparison Report
Generated: {timestamp}

## Summary
{tabulate(summary_data, headers=["Metric", "Value"], tablefmt="github")}

## Table Details
"""
        
        # Create detailed table
        table_data = []
        for result in self.results:
            if result["status"] == "ERROR":
                table_data.append([
                    result["table"], 
                    result["status"], 
                    "N/A", 
                    "N/A", 
                    "N/A",
                    result.get("error", "Unknown error")[:50] + "..."
                ])
            else:
                table_data.append([
                    result["table"],
                    result["status"],
                    result.get("total_diffs", 0),
                    result.get("added_rows", 0),
                    result.get("removed_rows", 0),
                    result.get("execution_time", "N/A")
                ])
        
        report += tabulate(table_data, 
                          headers=["Table", "Status", "Total Diffs", "Added", "Removed", "Duration"],
                          tablefmt="github")
        
        if self.summary["errors"]:
            report += "\n\n## Errors\n"
            for error in self.summary["errors"]:
                report += f"- {error}\n"
        
        return report
    
    def export_results(self, summary_only: bool = False, export_csv: bool = True, export_json: bool = True):
        """Export comparison results"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Generate and save summary report
        summary_report = self.generate_summary_report()
        summary_file = self.output_dir / f"summary_{timestamp}.md"
        with open(summary_file, 'w') as f:
            f.write(summary_report)
        self.logger.info(f"Summary report saved to: {summary_file}")
        
        if not summary_only:
            # Export detailed results to JSON
            if export_json:
                json_file = self.output_dir / f"detailed_results_{timestamp}.json"
                with open(json_file, 'w') as f:
                    json.dump({
                        "summary": self.summary,
                        "results": self.results,
                        "timestamp": datetime.now().isoformat()
                    }, f, indent=2)
                self.logger.info(f"Detailed JSON results saved to: {json_file}")
            
            # Export to CSV
            if export_csv and self.results:
                csv_file = self.output_dir / f"table_comparison_{timestamp}.csv"
                fieldnames = ["table", "status", "total_diffs", "added_rows", "removed_rows", "execution_time", "timestamp"]
                
                with open(csv_file, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    for result in self.results:
                        row = {k: result.get(k, '') for k in fieldnames}
                        writer.writerow(row)
                self.logger.info(f"CSV results saved to: {csv_file}")
        
        return summary_file

    def export_to_snowflake(self, validation_table: str = None):
        """Export comparison results to Snowflake validation table"""
        if not validation_table:
            validation_table = self.config.get('output', {}).get('validation_table', 'VALIDATION_RESULTS')
        
        # Use the new environment for storing validation results
        try:
            self.logger.info(f"Exporting results to Snowflake table: {validation_table}")
            
            # Create connection to new environment
            sf_config = self.config['new_snowflake']
            conn = snowflake.connector.connect(
                account=sf_config['account'],
                user=sf_config['user'],
                password=sf_config['password'],
                warehouse=sf_config['warehouse'],
                database=sf_config['database'],
                schema=sf_config['schema'],
                role=sf_config['role']
            )
            
            # Prepare data for export
            validation_data = []
            run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            for result in self.results:
                validation_data.append({
                    'RUN_ID': run_id,
                    'TABLE_NAME': result['table'],
                    'COMPARISON_STATUS': result['status'],
                    'TOTAL_DIFFS': result.get('total_diffs', 0),
                    'ADDED_ROWS': result.get('added_rows', 0),
                    'REMOVED_ROWS': result.get('removed_rows', 0),
                    'EXECUTION_TIME_SEC': result.get('execution_time', '0'),
                    'ERROR_MESSAGE': result.get('error', None),
                    'COMPARISON_TIMESTAMP': result['timestamp'],
                    'LEGACY_ENVIRONMENT': result.get('legacy_environment', 'unknown'),
                    'NEW_ENVIRONMENT': result.get('new_environment', 'unknown'),
                    'LEGACY_ACCOUNT': self.config['legacy_snowflake']['account'],
                    'NEW_ACCOUNT': self.config['new_snowflake']['account'],
                    'LEGACY_DATABASE': self.config['legacy_snowflake']['database'],
                    'NEW_DATABASE': self.config['new_snowflake']['database'],
                    'LEGACY_SCHEMA': self.config['legacy_snowflake']['schema'],
                    'NEW_SCHEMA': self.config['new_snowflake']['schema']
                })
            
            # Convert to DataFrame
            df = pd.DataFrame(validation_data)
            
            # Create table if it doesn't exist
            cursor = conn.cursor()
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {validation_table} (
                RUN_ID VARCHAR(50),
                TABLE_NAME VARCHAR(100),
                COMPARISON_STATUS VARCHAR(20),
                TOTAL_DIFFS INTEGER,
                ADDED_ROWS INTEGER,
                REMOVED_ROWS INTEGER,
                EXECUTION_TIME_SEC VARCHAR(50),
                ERROR_MESSAGE TEXT,
                COMPARISON_TIMESTAMP TIMESTAMP_LTZ,
                LEGACY_ENVIRONMENT VARCHAR(200),
                NEW_ENVIRONMENT VARCHAR(200),
                LEGACY_ACCOUNT VARCHAR(100),
                NEW_ACCOUNT VARCHAR(100),
                LEGACY_DATABASE VARCHAR(100),
                NEW_DATABASE VARCHAR(100),
                LEGACY_SCHEMA VARCHAR(100),
                NEW_SCHEMA VARCHAR(100),
                CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
            cursor.execute(create_table_sql)
            
            # Write DataFrame to Snowflake
            success, nchunks, nrows, _ = write_pandas(
                conn, df, validation_table, auto_create_table=False
            )
            
            if success:
                self.logger.info(f"Successfully exported {nrows} rows to {validation_table}")
            else:
                self.logger.error(f"Failed to export data to {validation_table}")
                
            conn.close()
            
        except Exception as e:
            error_msg = f"Error exporting to Snowflake: {str(e)}"
            self.logger.error(error_msg)
            self.summary["errors"].append(error_msg)

    def get_exit_code(self) -> int:
        """Get appropriate exit code based on results"""
        if self.summary["errors"] or self.summary["failed_tables"] > 0:
            return 1
        return 0


@click.command()
@click.option('--config', '-c', default='config.yaml', 
              help='Path to YAML configuration file')
@click.option('--out', '-o', default='outputs', 
              help='Output directory for reports')
@click.option('--logs', '-l', default='logs',
              help='Log directory')
@click.option('--summary-only', '-s', is_flag=True,
              help='Generate summary report only, skip detailed diffs')
@click.option('--export-csv/--no-export-csv', default=True,
              help='Export results to CSV format')
@click.option('--export-json/--no-export-json', default=True,
              help='Export results to JSON format')
@click.option('--export-snowflake', is_flag=True,
              help='Export results to Snowflake validation table')
@click.option('--verbose', '-v', is_flag=True,
              help='Enable verbose logging')
def main(config, out, logs, summary_only, export_csv, export_json, export_snowflake, verbose):
    """
    Snowflake Data Comparison Framework
    
    Compare data between old (SSIS) and new (Airflow/dbt) Snowflake schemas
    using row-by-row comparison to validate pipeline migration.
    """
    try:
        # Initialize comparer
        comparer = SnowflakeDataComparer(config, out, logs)
        
        if verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        
        # Perform comparisons
        comparer.logger.info("Starting table comparisons...")
        results = comparer.compare_all_tables()
        
        # Export results
        comparer.export_results(
            summary_only=summary_only,
            export_csv=export_csv,
            export_json=export_json
        )
        
        # Export to Snowflake if requested
        if export_snowflake:
            comparer.export_to_snowflake()
        
        # Print final summary to console
        print("\n" + "="*60)
        print("FINAL SUMMARY")
        print("="*60)
        print(f"Total Tables: {comparer.summary['total_tables']}")
        print(f"Passed: {comparer.summary['passed_tables']}")
        print(f"Failed: {comparer.summary['failed_tables']}")
        print(f"Errors: {len(comparer.summary['errors'])}")
        
        if comparer.summary['failed_tables'] > 0:
            print(f"\n⚠️  {comparer.summary['failed_tables']} table(s) have differences!")
        else:
            print(f"\n✅ All tables match perfectly!")
            
        exit_code = comparer.get_exit_code()
        sys.exit(exit_code)
        
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        sys.exit(2)


if __name__ == '__main__':
    main()