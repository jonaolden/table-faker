# Sample Configs

This file contains minimal and advanced YAML examples for Table Faker.

Full example: [`tests/test_table.yaml`](table-faker/tests/test_table.yaml:1)

## Minimal example
```yaml
tables:
  - table_name: person
    columns:
      - column_name: id
        data: row_id
      - column_name: first_name
        data: fake.first_name()
      - column_name: last_name
        data: fake.last_name()
```

## Advanced example
```yaml
version: 1
config:
  locale: en_US
  python_import:
    - dateutil
    - faker-education
  community_providers:
    - faker_education.SchoolProvider
tables:
  - table_name: person
    row_count: 10
    start_row_id: 101
    export_file_count: 3
    columns:
      - column_name: id
        data: row_id
        is_primary_key: true
      - column_name: first_name
        data: fake.first_name()
        type: string
      - column_name: last_name
        data: fake.last_name()
        type: string
      - column_name: full_name
        data: first_name + " " + last_name
      - column_name: age
        data: fake.random_int(18, 90)
        type: int32
      - column_name: discount_eligibility
        data: |
          if age < 25 or age > 60:
            return True
          else:
            return False
  - table_name: employee
    row_count: 10
    export_file_row_count: 60
    columns:
      - column_name: id
        data: row_id
      - column_name: person_id
        data: foreign_key("person", "id")
      - column_name: full_name
        data: foreign_key("person", "full_name")
      - column_name: hire_date
        data: fake.date_between()
        type: string
      - column_name: school
        data: fake.school_name()
      - column_name: level
        data: fake.school_level()
```

Notes:
- Parent tables must be defined before child tables.
- Use [`table-faker/README.md`](table-faker/README.md:1) for quickstart and general config.