# ![icon](media/tablefaker-icon-32.png) Table Faker

tablefaker is a lightweight Python tool to generate synthetic tabular data from a YAML schema.

Quick links:

- Full YAML reference: [`docs/yaml_reference.md`](docs/yaml_reference.md:1)
- Basic and advanced configuration example: [`docs/sample-configs.md`](docs/sample-configs.md:1)
- Domain-specific example: [`domains/hotel/hotel.yaml`](domains/hotel/hotel.yaml:1)
- Custom providers: [`docs/custom-providers.md`](docs/custom-providers.md:1)
- Custom functions: [`docs/custom-functions.md`](docs/custom-functions.md:1)

Key features:
- Schema-driven YAML config: Specify tables, columns and data type for generation. 
- Faker-based generators: use built-in functions, register community providers, or create custom Python functions to generate data.
- Referential integrity: generates parent tables before child tables to ensure data integrity. Supports multi-level FK relationships.
- Realistic foreign-key distributions: supports uniform, zipf, weighted_parent to mimic real-world data.
- Multiple output formats: csv, json, parquet, excel, sql, deltalake. 

Installation:
```bash
pip install -e .
```

Quickstart:
```bash
# generate CSVs to current folder
tablefaker --config tests/test_basic_table.yaml
```

CLI flags:
- --config PATH (required) — path to your YAML config file, see 
- --file_type (csv,json,parquet,excel,sql,deltalake) default: csv
- --target PATH — output folder or file
- --seed INT — make generation deterministic
- --infer-attrs true|false — enable name-based attribute inference
- --relationships — write relationships YAML
- --semantic-view — write semantic view YAML (Snowflake-compatible)

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

Notes:
- Parent tables must be defined before child tables.
- See [`docs/yaml_reference.md`](docs/yaml_reference.md:1) for full YAML schema reference.
- See [`docs/sample-configs.md`](docs/sample-configs.md:1) for a collection of sample configurations.
- See entries in domains/ for domain-specific example configurations.

Advanced features:
- Relationships YAML extraction — generate a yaml file with inferred table relationships with --relationships. See [`docs/relationships.md`](docs/relationships.md:1).
- Semantic View YAML generation — produce Snowflake-compatible semantic view YAML with --semantic-view. See [`docs/semantic-view.md`](docs/semantic-view.md:1).
  Note: Semantic view generation uses LLM to provide descriptions, requiring an `llm.config` (see [`docs/llm-config.md`](docs/llm-config.md:1) and [`table-faker/llm.config.example`](table-faker/llm.config.example:1)).
- Plugin provider loading — add packages or local modules via `config.python_import` and register community providers in `config.community_providers`. See [`docs/custom-providers.md`](docs/custom-providers.md:1) and [`table-faker/tablefaker/plugin_loader.py`](table-faker/tablefaker/plugin_loader.py:1).
- Custom foreign-key distributions — support for distributions such as `zipf` and `weighted_parent` for realistic FK sampling. See [`docs/distributions.md`](docs/distributions.md:1) and [`dev/instructions.md`](dev/instructions.md:1).
- Enhanced CLI features — seed control, `--infer-attrs`, `--relationships`, `--semantic-view`, `--generate-metrics`, and other flags. See [`table-faker/tablefaker/cli.py`](table-faker/tablefaker/cli.py:1).
- Copy-from-FK expression logic — use `copy_from_fk(...)` and the `auto` inference mode to copy attributes from parent rows. See [`docs/relationships.md`](docs/relationships.md:1) and [`table-faker/tablefaker/tablefaker.py`](table-faker/tablefaker/tablefaker.py:1).
- Business metrics generation — derive business metrics from semantic view generation (LLM-assisted). See [`docs/metrics.md`](docs/metrics.md:1) and [`table-faker/tablefaker/semantic_model_metrics.py`](table-faker/tablefaker/semantic_model_metrics.py:1).