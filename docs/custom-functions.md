# Custom Functions

Table Faker allows local Python functions to be used in YAML configs.

1. Create a local .py file next to your YAML .
2. Define functions that return a single value.
3. Reference them in your YAML under key `python_import` as `<module>.<function>()` (note: do not include .py extension in `python_import`). 

Example local module: `some_custom_function.py`

```python
from faker import Faker
fake = Faker()

def get_level():
    return f"level {fake.random_int(1, 5)}"
```

Example YAML:
```yaml
version: 1
config:
  python_import:
    - some_custom_function
tables:
  - table_name: employee
    row_count: 5
    columns:
      - column_name: id
        data: row_id
      - column_name: level
        data: some_custom_function.get_level()
```

Notes:
- Local modules must be located in the same folder as the YAML config.
- Custom functions can access `fake`, `random`, and other available evaluation context if imported in the module.
- Multi-line Python blocks are also supported directly in YAML using `data: |` and must end with a `return` statement.

See full examples: ['tests/test_plugin.yaml'](table-faker\tests\test_plugin.yaml) and [`domains/hotel/hotel.yaml`](table-faker/domains/hotel/hotel.yaml:1)