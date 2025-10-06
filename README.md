# ![icon](media/tablefaker-icon-32.png) Table Faker

tablefaker is a lightweight Python tool to generate realistic synthetic datasets from a YAML schema, for testing, demos, and prototyping.

Key features:
- Schema-driven YAML config: Specify tables, columns and data type for generation in a simple YAML format.
- Faker-based generators: use built-in functions, community providers, or create custom Python functions to generate data tailored to your needs.
- Referential integrity: The tool generates parent tables before child tables to ensure data integrity. Supports multi-level FK relationships.
- Realistic foreign-key distributions: Generation supports distribution strategies (uniform, zipf, and weighted_parent) to mimic real-world data and 
- Multiple output formats: csv, json, parquet, excel, sql, deltalake (streaming).

Installation:
```bash
pip install -e .
```

Quickstart:
```bash
# generate CSVs to current folder
tablefaker --config tests/test_basic_table.yaml
```

CLI flags (see `tablefaker --help` for full list):

- `--config <PATH>` (required)
  Path to your YAML config file. See [`docs/yaml-reference.md`](docs/yaml-reference.md:1) for full schema and examples.

- `--file_type <extension>` (choices: csv, json, parquet, excel, sql, deltalake) (default: csv)
  Output file format to generate.

- `--target <PATH|DIR>`
  Output destination. If a directory is provided, multiple files will be written into it; if a single file path is provided, output will be written to that file.

- `--seed <INT>`
  Use a numeric seed to make generation deterministic and reproducible.

- `--infer-attrs <true|false>` (default: false)
  Enable name-based attribute inference for columns (attempts to infer semantic attributes from column names).

- `--relationships`
  Generate and write a relationships YAML alongside the generated outputs.

- `--semantic-view`
  Generate and write a Snowflake-compatible semantic view YAML.

Minimal YAML: 
```yaml
version: 1
config:
  locale: en_US
tables:
  - table_name: person
    row_count: 100
    columns:
      - column_name: id
        data: row_id
      - column_name: first_name
        data: fake.first_name()
```

- For full YAML reference, see [`docs/yaml_reference.md`](docs/yaml-reference.md:1)
- Basic and advanced configuration example: [`docs/sample-configs.md`](docs/sample-configs.md:1)
- Domain-specific example: [`domains/hotel/hotel.yaml`](domains/hotel/hotel.yaml:1)

Notes:
- Parent tables must be defined before child tables.

Advanced features:
- Relationships YAML extraction - generate a yaml file with inferred table relationships with `--relationships`. See [`docs/relationships.md`](docs/relationships.md:1).
- Streaming server - continuous, dependency-aware streaming to Delta/Parquet, see detailed usage in [`docs/streaming-server.md`](docs/streaming-server.md:1).
- Semantic View YAML generation - produce Snowflake-compatible semantic view YAML with `--semantic-view`. See [`docs/semantic-view.md`](docs/semantic-view.md:1).
  Note: Semantic view generation uses LLM to provide descriptions, requiring an `llm.config` (see [`docs/llm-config.md`](docs/llm-config.md:1) and [`table-faker/llm.config.example`](table-faker/llm.config.example:1)).
- Plugin provider loading - add packages or local modules via `config.python_import` and register community providers in `config.community_providers`. See [`docs/custom-providers.md`](docs/custom-providers.md:1) for details.
- Custom functions: [`docs/custom-functions.md`](docs/custom-functions.md:1)