# Streaming Data Generator Server

A continuous data generation server that appends rows to Delta/Parquet tables at configurable per-table cadences while maintaining FK consistency.

## Features

- **Per-table update policies**: Control which tables update (`append`, `disabled`, `replace`)
- **Configurable cadence**: Set rows-per-minute for each table independently
- **FK consistency**: Automatically loads existing data into caches for [`foreign_key()`](../tablefaker/tablefaker.py:384) and [`copy_from_fk()`](../tablefaker/tablefaker.py:48)
- **Atomic appends**: Uses Delta Lake for ACID guarantees
- **Graceful shutdown**: Handles Ctrl+C cleanly
- **Deterministic**: Supports seed-based generation
- **Multi-threaded**: Each table runs on its own thread

## Installation

```bash
# Required dependencies
pip install deltalake pandas pyarrow

# Verify tablefaker is installed
pip install -e table-faker/
```

## Quick Start

1. **Create your configuration** (see [`streaming_config.yaml`](streaming_config.yaml) for example):

```yaml
version: 1
config:
  locale: en_US
  seed: 42
tables:
  - table_name: person
    row_count: 100
    update_policy: append
    cadence:
      rows_per_minute: 60
      enabled: true
    columns:
      - column_name: id
        data: row_id
        is_primary_key: true
      - column_name: email
        data: fake.email()
```

2. **Run the server**:

```bash
cd table-faker/examples
python streaming_server.py --config streaming_config.yaml --output ./delta_tables
```

3. **Stop gracefully**: Press `Ctrl+C`

## Configuration Reference

### Table-Level Settings

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `update_policy` | string | `append`, `disabled`, or `replace` | `append` |
| `cadence.rows_per_minute` | int | Rows to generate per minute | `60` |
| `cadence.enabled` | bool | Enable/disable updates for this table | `true` |
| `start_row_id` | int | Starting row ID (auto-continues from existing data) | `1` |

### Update Policies

- **`append`**: Continuously add new rows (recommended for event/log tables)
- **`disabled`**: Never update (for static reference tables like countries)
- **`replace`**: Full table replacement each cycle (not yet implemented)

### Example: Multi-table with Different Cadences

```yaml
tables:
  - table_name: high_volume_events
    cadence:
      rows_per_minute: 600  # High frequency
      
  - table_name: daily_summaries
    cadence:
      rows_per_minute: 1    # Low frequency
      
  - table_name: countries
    update_policy: disabled  # Static
```

## How It Works

### Architecture

```
StreamingServer
  ├── TableFaker (shared instance)
  │   ├── primary_key_cache (for FK lookups)
  │   └── parent_rows (for copy_from_fk)
  └── StreamingTableGenerator (per table)
      ├── Load existing Delta data → caches
      ├── Generate batch every N seconds
      └── Append to Delta table atomically
```

### Startup Sequence

1. Load YAML config and apply seed
2. Create `StreamingTableGenerator` per table
3. Load existing Delta tables into `primary_key_cache` and `parent_rows` (see [`streaming_server.py`](streaming_server.py:97))
4. Start generation threads for enabled tables
5. Each thread generates batches at `tick_interval` (10 seconds)

### FK Consistency

The server maintains FK relationships by:
- Loading existing parent table data on startup (see [`streaming_server.py`](streaming_server.py:97))
- Populating [`TableFaker.primary_key_cache`](../tablefaker/tablefaker.py:19) and [`TableFaker.parent_rows`](../tablefaker/tablefaker.py:21)
- Using existing generation logic (see [`tablefaker.py`](../tablefaker/tablefaker.py:384) for FK selection)
- Child tables reference parent PKs via [`foreign_key()`](../tablefaker/tablefaker.py:384) calls

### Example FK Workflow

```yaml
tables:
  - table_name: customers
    update_policy: append
    cadence:
      rows_per_minute: 10
    columns:
      - {column_name: id, data: row_id, is_primary_key: true}
      - {column_name: email, data: fake.email()}
      
  - table_name: orders
    update_policy: append
    cadence:
      rows_per_minute: 50  # More orders than customers
    columns:
      - {column_name: order_id, data: row_id, is_primary_key: true}
      - {column_name: customer_id, data: foreign_key("customers", "id")}
      - {column_name: customer_email, data: copy_from_fk("customer_id", "customers", "email")}
```

On startup, the server:
1. Loads existing `customers` rows → `primary_key_cache["customers"]["id"]`
2. Loads existing `orders` rows (no cache needed)
3. Starts generating new customers (10/min)
4. Starts generating new orders (50/min) that reference existing + new customers

## Usage Examples

### Basic Usage

```bash
# Run with default output directory
python streaming_server.py --config my_config.yaml

# Specify output directory
python streaming_server.py --config my_config.yaml --output /data/delta
```

### Reading Generated Data

```python
from deltalake import DeltaTable
import pandas as pd

# Read a table
dt = DeltaTable("./delta_tables/person")
df = dt.to_pandas()
print(f"Total rows: {len(df)}")
print(df.head())

# Query with filters
df_filtered = dt.to_pandas(filters=[("age", ">", 30)])
```

### Monitoring

```python
# Check table stats
from deltalake import DeltaTable

dt = DeltaTable("./delta_tables/orders")
print(f"Version: {dt.version()}")
print(f"Files: {dt.files()}")
print(f"History: {dt.history()}")
```

## Production Considerations

### State Persistence

The server keeps state in-memory. For production:
- Use external orchestration (Airflow, Kubernetes CronJobs)
- Persist `primary_key_cache` and `parent_rows` to disk periodically
- Implement checkpoint/recovery logic

### Scalability

- Each table runs on a separate thread (GIL-limited in Python)
- For high-volume: run multiple processes, one per table
- Consider async I/O for Delta writes

### Monitoring

Add monitoring for:
- Rows generated per table per minute
- Delta table size growth
- Generation errors/retries
- Memory usage of caches

### Example: Docker Deployment

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY table-faker/ ./table-faker/
COPY examples/ ./examples/

CMD ["python", "examples/streaming_server.py", \
     "--config", "/config/streaming_config.yaml", \
     "--output", "/data/delta"]
```

## Troubleshooting

### "Missing parent row" Error

**Cause**: Child table references FK before parent table generated rows.

**Solution**: 
1. Ensure parent tables are listed first in YAML
2. Set parent `row_count` > 0 for initial generation
3. Check startup logs confirm parent cache loaded

### High Memory Usage

**Cause**: `parent_rows` cache grows with parent table size.

**Solutions**:
- Limit cache to recent N rows only
- Use external key-value store (Redis)
- Implement LRU eviction policy

### Slow Appends

**Cause**: Delta write overhead, especially with many small batches.

**Solutions**:
- Increase `tick_interval` (fewer, larger batches)
- Tune Delta table properties (`delta.autoOptimize.optimizeWrite`)
- Use Parquet instead of Delta for append-only workloads

## Advanced: Custom Distributions

Use distribution parameters for realistic FK patterns (see [`tablefaker.py`](../tablefaker/tablefaker.py:384)):

```yaml
- column_name: customer_id
  data: foreign_key("customers", "id", distribution="zipf", param=1.2)
  # Popular customers get more orders (Zipf distribution)
```

## API Reference

### StreamingServer

```python
server = StreamingServer(config_path, output_path)
server.start()  # Blocks until Ctrl+C
server.stop()   # Graceful shutdown
```

### StreamingTableGenerator

```python
generator = StreamingTableGenerator(table_config, table_faker, configurator, output_path)
generator.load_existing_data()  # Populate caches
generator.start()  # Start thread
generator.stop()   # Stop thread
```

## Related

- [`tablefaker.py`](../tablefaker/tablefaker.py) - Core generation logic
- [`streaming_config.yaml`](streaming_config.yaml) - Example configuration
- Delta Lake docs: https://delta.io/
- TableFaker README: [`../README.md`](../README.md)