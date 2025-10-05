# Semantic View Generation

Generate a semantic view YAML describing tables, entities and metrics using an LLM-assisted pipeline.

## Purpose

The semantic view feature converts a tablefaker YAML into a higher-level semantic model (entities, primary keys, natural keys, metrics) helpful for analytics and BI tools. Primary use case is generating Snowflake-compatible semantic view YAML, to test Snowflake Intelligence. 

## Requirements

- Provide an LLM configuration file at [`table-faker/llm.config`](table-faker/llm.config:1) or use the example [`table-faker/llm.config.example`](table-faker/llm.config.example:1).
- Internet access or a local LLM endpoint as configured in your llm.config.

## How it works

1. Parse tables and column metadata from your YAML.
2. Extract relationships and candidate keys.
3. Call the LLM to generate human-friendly names, descriptions and suggested metrics.
4. Emit a semantic view YAML file alongside your config.

Internals implemented in [`table-faker/tablefaker/semantic_view.py`](table-faker/tablefaker/semantic_view.py:1).

## CLI Usage

Run semantic view generation via the CLI:

```bash
tablefaker --config path/to/config.yaml --semantic-view --output path/to/output.yml
```

Or using the Python module:

```python
from tablefaker import tablefaker
tablefaker.to_target("json", "config.yaml", target_file_path="semantic_view.yml", semantic_view=True)
```

Generated file: by default the semantic view is written to `./config_semantic_view.yml` next to the config unless overridden.

## Example output (excerpt)

```yaml
entities:
  - name: person
    primary_key:
      columns: [id]
    description: Person entity with contact info
  - name: orders
    primary_key:
      columns: [order_id]
    description: Order events linked to person
```

## Notes and best practices

- Ensure `is_primary_key` is set for key columns to get accurate keys in the semantic view.
- Use concise and descriptive column names in your YAML to improve LLM output quality.
- Review and if needed edit the generated semantic view before adding it to Snowflake.

## Troubleshooting

- If generation fails, check your llm.config and API credentials.
- If the LLM produces poor summaries, add review column naming, increase model temperature in llm.config or consider using a different model.

## Related

- Implementation: [`table-faker/tablefaker/semantic_view.py`](table-faker/tablefaker/semantic_view.py:1)
- LLM config example: [`table-faker/llm.config.example`](table-faker/llm.config.example:1)