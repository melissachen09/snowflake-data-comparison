# Snowflake Data Comparison Framework

A robust, scalable framework for validating data consistency between legacy SSIS pipelines and modern Airflow/dbt pipelines across **separate Snowflake environments**. Uses row-by-row comparison to ensure data integrity during pipeline migrations.

## 🎯 Overview

This framework leverages the open-source [Datafold data-diff](https://github.com/datafold/data-diff) library to perform efficient row-level comparisons between **two separate Snowflake accounts/environments**. It's specifically designed to validate that new pipeline implementations in a modern environment produce exactly the same results as legacy systems in the original environment.

### Key Features

- **Cross-environment comparison**: Compare data between separate Snowflake accounts/environments
- **Row-by-row comparison**: Similar to RedGate Data Compare, but open-source
- **High scalability**: Handles millions to billions of rows efficiently using checksum-based divide-and-conquer
- **Comprehensive reporting**: Summary reports, row-level diffs, and multiple export formats
- **Flexible configuration**: YAML-based configuration for easy setup
- **Multiple output formats**: Markdown reports, CSV, JSON, and Snowflake tables
- **Detailed logging**: Comprehensive logging for troubleshooting and audit trails
- **Exit codes**: Proper exit codes for integration with CI/CD pipelines

### Performance

- Verifies 25M+ rows in under 10 seconds when tables are identical
- Scales to billions of rows in minutes
- Uses segment checksums to avoid scanning identical data portions

## 📁 Project Structure

```
snowflake-data-compare/
├── compare.py           # Main CLI entry point
├── config.yaml          # Configuration file
├── requirements.txt     # Python dependencies
├── logs/               # Daily execution logs
├── outputs/            # Comparison reports and exports
└── README.md           # This file
```

## 🚀 Quick Start

### 1. Installation

```bash
# Clone or create the project directory
cd snowflake-data-compare

# Install dependencies
pip install -r requirements.txt

# Or install directly
pip install data-diff[snowflake] snowflake-connector-python PyYAML click pandas tabulate
```

### 2. Configuration

Edit `config.yaml` with your Snowflake connection details for both environments and tables to compare:

```yaml
# Legacy Environment (SSIS Pipeline Output)
legacy_snowflake:
  account: "legacy_account.snowflakecomputing.com"
  user: "legacy_username"
  password: "legacy_password"
  warehouse: "legacy_warehouse"
  role: "legacy_role"
  database: "legacy_database"
  schema: "production"

# New Environment (Airflow/dbt Pipeline Output)
new_snowflake:
  account: "new_account.snowflakecomputing.com"
  user: "new_username"
  password: "new_password"
  warehouse: "new_warehouse"
  role: "new_role"
  database: "new_database"
  schema: "production"

tables:
  - name: CUSTOMERS
    keys: [customer_id]
    # Exclude specific columns from comparison (audit columns, timestamps, etc.)
    exclude_columns: [updated_at, created_at, etl_timestamp, last_modified_by]
  - name: ORDERS
    keys: [order_id]
    exclude_columns: [created_date, modified_date, source_system_id]

# Global exclusions applied to ALL tables
global_exclude_columns:
  - etl_insert_date
  - etl_update_date
  - etl_batch_id
  - _fivetran_synced
  - _dbt_source_relation
```

### 3. Basic Usage

```bash
# Run comparison with default settings
python compare.py

# Run with custom config file
python compare.py --config my_config.yaml

# Generate summary report only (faster)
python compare.py --summary-only

# Export results to Snowflake validation table
python compare.py --export-snowflake

# Verbose logging for troubleshooting
python compare.py --verbose
```

## 📋 CLI Options

```bash
Usage: compare.py [OPTIONS]

Options:
  -c, --config TEXT     Path to YAML configuration file [default: config.yaml]
  -o, --out TEXT        Output directory for reports [default: outputs]
  -l, --logs TEXT       Log directory [default: logs]
  -s, --summary-only    Generate summary report only, skip detailed diffs
  --export-csv          Export results to CSV format [default: True]
  --export-json         Export results to JSON format [default: True]
  --export-snowflake    Export results to Snowflake validation table
  -v, --verbose         Enable verbose logging
  --help               Show this message and exit
```

## ⚙️ Configuration Reference

### Legacy Environment Connection

```yaml
legacy_snowflake:
  account: "legacy_account.snowflakecomputing.com" 
  user: "USERNAME"
  password: "PASSWORD"
  warehouse: "LEGACY_WH"
  role: "LEGACY_ROLE"
  database: "LEGACY_DB"
  schema: "production"  # Schema containing SSIS pipeline output
```

### New Environment Connection

```yaml
new_snowflake:
  account: "new_account.snowflakecomputing.com"
  user: "USERNAME"
  password: "PASSWORD"
  warehouse: "NEW_WH"
  role: "NEW_ROLE"
  database: "NEW_DB"
  schema: "production"  # Schema containing Airflow/dbt pipeline output
```

### Table Configuration

```yaml
tables:
  - name: TABLE_NAME
    keys: [primary_key]                    # Required: columns that uniquely identify rows
    exclude_columns: [col1, col2]          # Optional: table-specific columns to ignore
```

### Global Column Exclusions

```yaml
global_exclude_columns:
  # Applied to ALL tables in addition to table-specific exclusions
  - etl_insert_date
  - etl_update_date
  - etl_batch_id
  - created_by_system
  - updated_by_system
  - _fivetran_synced    # Tool-specific columns
  - _dbt_source_relation
  - _airbyte_ab_id
```

**Common columns to exclude:**
- **Audit columns**: `created_date`, `modified_date`, `updated_by`
- **ETL metadata**: `etl_batch_id`, `load_timestamp`, `process_id`
- **System-generated**: `record_hash`, `surrogate_key`, `row_number`
- **Tool-specific**: `_fivetran_synced`, `_dbt_source_relation`, `_airbyte_emitted_at`
- **Timestamps**: `row_insert_timestamp`, `last_processed_date`

### Comparison Settings

```yaml
comparison:
  max_diffs: 1000        # Maximum number of differences to show per table
  include_row_diffs: true # Include row-level diffs in output
  timeout_seconds: 300    # Timeout for each table comparison
```

### Output Settings

```yaml
output:
  summary_report: true
  export_csv: true
  export_to_snowflake: false
  validation_table: "VALIDATION_RESULTS"  # Snowflake table for results
```

## 📊 Output Formats

### 1. Summary Report (Markdown)
- High-level comparison metrics
- Table-by-table status
- Execution time per table
- Error summary

### 2. Detailed JSON Export
- Complete comparison results
- Row-level differences
- Metadata and timestamps

### 3. CSV Export
- Tabular format for spreadsheet analysis
- Summary statistics per table

### 4. Snowflake Validation Table
- Results stored directly in Snowflake
- Queryable history of all comparison runs
- Integration with data monitoring dashboards

## 🔍 Understanding Results

### Status Codes
- **PASS**: Tables are identical
- **FAIL**: Differences found between tables  
- **ERROR**: Comparison failed due to technical issues

### Difference Types
- **Added rows** (`+`): Rows present in new schema but missing in old
- **Removed rows** (`-`): Rows present in old schema but missing in new
- **Changed rows**: Rows with same keys but different values

### Exit Codes
- `0`: All comparisons passed successfully
- `1`: One or more tables have differences or errors occurred
- `2`: Fatal error (configuration issues, connection failures)

## 🔧 Advanced Usage

### Column Exclusions

The framework supports two levels of column exclusions:

#### 1. Table-Specific Exclusions
Exclude columns for specific tables only:

```yaml
tables:
  - name: CUSTOMER_DIM
    keys: [customer_key]
    exclude_columns: [last_updated_date, created_by_user, audit_timestamp]
```

#### 2. Global Exclusions
Exclude columns from ALL table comparisons:

```yaml
global_exclude_columns:
  - etl_insert_date
  - etl_update_date  
  - etl_batch_id
  - record_hash
```

#### 3. Combined Usage
Both exclusion types work together. The framework will exclude:
- All columns listed in `global_exclude_columns`
- Plus any columns listed in table-specific `exclude_columns`
- Duplicates are automatically removed

**Example log output with exclusions:**
```
INFO - Comparing table: CUSTOMERS
INFO - Excluding columns: etl_insert_date, etl_update_date, created_date, modified_date
INFO - ✓ CUSTOMERS: PASSED - No differences found
```

### Running Specific Tables

Modify your config to include only the tables you want to compare:

```yaml
tables:
  - name: CRITICAL_TABLE
    keys: [id]
```

### Custom Connection Strings

For advanced Snowflake configurations, you can modify the connection logic in `compare.py`:

```python
# Example: Adding custom connection parameters
conn_params = {
    'account': config['account'],
    'user': config['user'],
    'password': config['password'],
    'warehouse': config['warehouse'],
    'role': config['role'],
    'database': config['database'],
    'session_parameters': {
        'QUERY_TAG': 'DATA_VALIDATION'
    }
}
```

### Automated Daily Runs

Create a shell script for daily execution:

```bash
#!/bin/bash
cd /path/to/snowflake-data-compare

# Run comparison
python compare.py --export-snowflake --verbose

# Check exit code
if [ $? -eq 0 ]; then
    echo "✅ Data validation passed"
else
    echo "❌ Data validation failed - check logs"
    # Send alert, create ticket, etc.
fi
```

### Integration with CI/CD

```yaml
# Example GitHub Actions workflow
name: Data Validation
on:
  schedule:
    - cron: '0 2 * * *'  # Run daily at 2 AM

jobs:
  validate-data:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.10'
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run data validation
        run: python compare.py --export-snowflake
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
```

## 🚨 Troubleshooting

### Common Issues

**Connection Errors**
- Verify Snowflake credentials and network access for **both environments**
- Check warehouses are running and accessible in both accounts
- Ensure roles have proper permissions on both schemas
- Test connectivity to both Snowflake accounts independently

**Memory Issues with Large Tables**
- Use `--summary-only` flag for initial assessment
- Consider comparing tables in batches
- Increase available memory for the Python process

**Timeout Errors**
- Increase `timeout_seconds` in configuration
- Check Snowflake warehouse size (larger warehouses = faster comparisons)
- Verify tables have proper indexes on key columns

**Permission Errors**
- Ensure roles have SELECT access to schemas in **both environments**
- For Snowflake export, role in the **new environment** needs CREATE TABLE and INSERT permissions
- Check database and schema usage permissions in both accounts
- Verify cross-account connectivity if using private endpoints

### Debug Mode

Enable verbose logging for detailed troubleshooting:

```bash
python compare.py --verbose
```

Check log files in the `logs/` directory for detailed execution information.

### Performance Optimization

1. **Use appropriate warehouse sizes** for your data volume in both environments
2. **Optimize key selection** - use indexed columns when possible
3. **Run during off-peak hours** to avoid resource contention in both accounts
4. **Consider incremental comparisons** for very large datasets
5. **Network latency** - Consider running the tool closer to your Snowflake regions

## 📈 Monitoring and Alerting

### Snowflake Validation Dashboard

Query the validation results table to create monitoring dashboards:

```sql
-- Daily summary view
SELECT 
    DATE(COMPARISON_TIMESTAMP) as comparison_date,
    COUNT(*) as total_tables,
    SUM(CASE WHEN COMPARISON_STATUS = 'PASS' THEN 1 ELSE 0 END) as passed_tables,
    SUM(CASE WHEN COMPARISON_STATUS = 'FAIL' THEN 1 ELSE 0 END) as failed_tables,
    SUM(TOTAL_DIFFS) as total_differences
FROM VALIDATION_RESULTS 
GROUP BY DATE(COMPARISON_TIMESTAMP)
ORDER BY comparison_date DESC;

-- Failed table details
SELECT *
FROM VALIDATION_RESULTS 
WHERE COMPARISON_STATUS != 'PASS' 
ORDER BY COMPARISON_TIMESTAMP DESC;
```

### Alerting Integration

Set up alerts based on validation results:

```python
# Example: Send alert on failures
if comparer.get_exit_code() != 0:
    send_slack_alert(f"Data validation failed: {comparer.summary['failed_tables']} tables")
```

## 🤝 Contributing

This framework is designed to be extensible. Common enhancements:

1. **Additional database connectors** (PostgreSQL, BigQuery, etc.)
2. **Enhanced reporting formats** (HTML, email reports)
3. **Custom validation rules** (data quality checks)
4. **Integration with data lineage tools**

## 📄 License

This project is open source. See the data-diff library license for dependency requirements.

## 🔗 References

- [Datafold data-diff GitHub](https://github.com/datafold/data-diff)
- [Snowflake Connector for Python](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector)
- [Click CLI Documentation](https://click.palletsprojects.com/)

---

**Need Help?** Check the logs directory for detailed execution logs, or review the troubleshooting section above.