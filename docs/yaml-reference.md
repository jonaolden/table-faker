## YAML schema reference 

```yaml
version: 1

config:
  locale: <locale_string>                      # e.g. en_US
  seed: <integer>                              # deterministic seed applied to random, numpy, Faker
  infer_entity_attrs_by_name: <true|false>     # enable `data: auto` name inference
  python_import:
    - <module_name>  | <local_file_name>        # python standard library module (must be installed) | local .py file without .py extension in the same folder as the yaml file
  community_providers:
    - <module_name>(<community_provider_name>)  # note that the module must be installed via pip and imported in python_import

tables:
  - table_name: <table_name>
    row_count: <integer>
    start_row_id: <integer>
    export_file_count: <integer>
    export_file_row_count: <integer>

    columns:
      - column_name: <column_name>              # string (required)
        data: <package>.<function_name>() | <hardcoded_value> | <column_reference> | auto | None
              # Examples of allowed data forms:
              #   <package>.<function_name>()   -> faker or other imported function call
              #   <hardcoded_value>            -> numeric or r"string"
              #   <column_reference>          -> reference another column by name (first_name + " " + last_name)
              #   auto                        -> resolved to copy_from_fk(...) when infer_entity_attrs_by_name is true
              #   None                        -> explicit NULL
              #   multi-line Python block using | (must return a value)
        is_primary_key: <true|false>
        type: string | int32 | int64 | float | boolean
        null_percentage: <float between 0.0 and 1.0>
        description: <string>

# Expression evaluation context
# Available variables inside `data` expressions:
#   fake, random, datetime, date, timedelta, time, timezone, tzinfo, UTC, MINYEAR, MAXYEAR, math, string, row_id
#
# Special helper functions:
#   foreign_key(parent_table, parent_column, distribution="uniform", param=None, parent_attr=None, weights=None)
#   copy_from_fk(fk_column, parent_table, parent_attr)
#
# Multi-line Python block:
# Use YAML block scalar `|` and include a final `return <value>` statement.

```