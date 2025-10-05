# Custom Faker Providers

Table Faker supports community and custom Faker providers. To use a provider:

1. Install the provider package with pip.
2. Add the package name (or local module) under `python_import`.
3. Register the provider class under `community_providers`.

Example YAML:
```yaml
version: 1
config:
  locale: en_US
  python_import:
    - faker-education # install this package via pip
  community_providers:
    - faker_education.SchoolProvider
tables:
  - table_name: employee
    row_count: 5
    columns:
      - column_name: id
        data: row_id
      - column_name: school
        data: fake.school_name()  # provided by SchoolProvider
```

Notes:
- Ensure the provider package is installed in the environment where you run Table Faker.
- `python_import` accepts standard packages or local `.py` modules (local modules should be next to your YAML file).
- Community providers extend the `fake` object; use their methods via `fake.<method>()`.

Resources:
- Community providers index: https://faker.readthedocs.io/en/master/communityproviders.html#
- Full example config: [`tests/test_table.yaml`](table-faker/tests/test_table.yaml:1)


