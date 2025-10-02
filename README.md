# ![icon](https://raw.githubusercontent.com/necatiarslan/table-faker/main/media/tablefaker-icon-32.png) Table Faker
![screenshoot](https://raw.githubusercontent.com/necatiarslan/table-faker/main/media/terminal.png)
**tablefaker** is a versatile Python package that enables effortless generation of realistic yet synthetic table data for various applications. Whether you need test data for software development, this tool simplifies the process with an intuitive schema definition in YAML format.

## Key Features
- **Schema Definition**: Define your table schema using a simple YAML file, specifying column names, data types, data generation logic, and relationships.
- **Data Generation**: Utilize the **Faker** library, including community providers, and custom python functions to generate authentic-looking, logically valid and deterministic datasets.
- **Data Relationships**: Create multiple tables with different schemas and data generation logic in a single YAML file. Define relationships between tables for foreign keys and primary keys.
- **Output Formats**:
  - Pandas DataFrame
  - SQL insert script
  - CSV
  - Parquet
  - JSON
  - Excel
  - Delta Lake

- **Schema Outputs**:
  - Relationships YAML file
  - Semantic view YAML file (partially Snowflake compatible)
  - Business metrics from semantic view YAML file (partially Snowflake compatible)

## Installation
Clone repository, then from the root folder, run:
```bash
pip install -e .
```

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

Notes:
- Parent tables must be defined before child tables.
- Two-phase evaluation resolves columns that reference other columns correctly.
- For a full example, see [`tests/test_table.yaml`](tests/test_table.yaml:1).

## Sample Yaml File Minimal
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
## Sample Yaml File Advanced
```yaml
version: 1
config:
  locale: en_US
  python_import:
    - dateutil
    - faker-education # custom faker provider
  community_providers:
    - faker_education.SchoolProvider # custom faker provider
tables:
  - table_name: person
    row_count: 10
    start_row_id: 101                               # you can set row_id starting point
    export_file_count: 3                           # you can set export file count (dominant to export_file_row_count)
    columns:
      - column_name: id
        data: row_id                                # row_id is a built-in function
        is_primary_key: true                        # define primary key to use as a foreign key
      - column_name: first_name
        data: fake.first_name()                     # faker function
        type: string
      - column_name: last_name
        data: fake.last_name()
        type: string
      - column_name: full_name
        data: first_name + " " + last_name           # use a column to generate a new column
        is_primary_key: true
      - column_name: age
        data: fake.random_int(18, 90)
        type: int32
      - column_name: street_address
        data: fake.street_address()
      - column_name: city
        data: fake.city()
      - column_name: state_abbr
        data: fake.state_abbr()
      - column_name: postcode
        data: fake.postcode()
      - column_name: gender
        data: random.choice(["male", "female"])     # random.choice is a built-in function
        null_percentage: 0.5                        # null_percentage is a built-in function
      - column_name: left_handed
        data: fake.pybool()
      - column_name: today
        data: datetime.today().strftime('%Y-%m-%d') # datetime package is available by default
      - column_name: easter_date
        data: dateutil.easter.easter(2025).strftime('%Y-%m-%d') # python package you need to import in python_import
      - column_name: discount_eligibility           # custom python function
        data: |
          if age < 25 or age > 60:
            return True
          else:
            return False
  - table_name: employee
    row_count: 10
    export_file_row_count: 60                      # you can set export file row count
    columns:
      - column_name: id
        data: row_id
      - column_name: person_id
        data: foreign_key("person", "id")          # get primary key from another table
      - column_name: full_name
        data: foreign_key("person", "full_name")
      - column_name: hire_date
        data: fake.date_between()
        type: string
      - column_name: title
        data: random.choice(["engineer", "senior engineer", "principal engineer", "director", "senior director", "manager", "vice president", "president"])
      - column_name: salary
        data: None #NULL
        type: float
      - column_name: height
        data: r"170 cm" #string
      - column_name: weight
        data: 150 #number
      - column_name: school
        data: fake.school_name() # community provider function
      - column_name: level
        data: fake.school_level() # community provider function
```
[full yml example](tests/test_table.yaml)


## Usage
Run tablefaker in your terminal to automate fake data generation. The CLI reads the YAML config and supports importing Python modules via `config.python_import` and adding Faker community providers declared under `config.community_providers` (see "Custom Faker Providers" below). Custom Python functions are supported when placed in the target yaml directory.

Supported CLI flags:
- --config : path to YAML or JSON config
- --file_type : csv,json,parquet,excel,sql,deltalake (default: csv)
- --target : target folder or file path
- --seed : integer seed to make generation deterministic
- --infer-attrs : "true" or "false" to override infer_entity_attrs_by_name
- -- relationships : generate relationships yaml file 
- --semantic-view : generate semantic view yaml file (Snowflake compatible)

```bash
# exports to current folder in csv format 
tablefaker --config tests/test_table.yaml

# exports as sql insert script files
tablefaker --config tests/test_table.yaml --file_type sql --target ./out

# exports to current folder in excel format
tablefaker --config tests/test_table.yaml --file_type excel

# exports all tables in json format to a folder
tablefaker --config tests/test_table.yaml --file_type json --target ./target_folder

# exports a single table to a parquet file
tablefaker --config tests/test_table.yaml --file_type parquet --target ./target_folder/target_file.parquet

# pass an explicit seed and enable attribute inference
tablefaker --config tests/test_table.yaml --seed 42 --infer-attrs true
```

## Sample CSV Output
```
id,first_name,last_name,age,dob,salary,height,weight
1,John,Smith,35,1992-01-11,,170 cm,150
2,Charles,Shepherd,27,1987-01-02,,170 cm,150
3,Troy,Johnson,42,,170 cm,150
4,Joshua,Hill,86,1985-07-11,,170 cm,150
5,Matthew,Johnson,31,1940-03-31,,170 cm,150
```

## Sample Sql Output
```sql
INSERT INTO employee
(id,person_id,hire_date,title,salary,height,weight,school,level)
VALUES
(1, 4, '2020-10-09', 'principal engineer', NULL, '170 cm', 150, 'ISLIP HIGH SCHOOL', 'level 2'),
(2, 9, '2002-12-20', 'principal engineer', NULL, '170 cm', 150, 'GUY-PERKINS HIGH SCHOOL', 'level 1'),
(3, 2, '1996-01-06', 'principal engineer', NULL, '170 cm', 150, 'SPRINGLAKE-EARTH ELEM/MIDDLE SCHOOL', 'level 3');
```
## Custom Faker Providers
You can add and use custom / community faker providers with table faker.\
Here is a list of these community providers.\
https://faker.readthedocs.io/en/master/communityproviders.html#

```yaml
version: 1
config:
  locale: en_US
  python_import:
    - faker-education # custom faker provider package
  community_providers:
    - faker_education.SchoolProvider # custom faker provider
tables:
  - table_name: employee
    row_count: 5
    columns:
      - column_name: id
        data: row_id
      - column_name: person_id
        data: fake.random_int(1, 10)
      - column_name: hire_date
        data: fake.date_between()
      - column_name: school
        data: fake.school_name()  # custom provider
```

## Custom Functions
With Table Faker, you have the flexibility to provide your own custom functions to generate column data. This advanced feature empowers developers to create custom fake data generation logic that can pull data from a database, API, file, or any other source as needed.\
You can also supply multiple functions in a list, allowing for even more versatility. \
The custom function you provide should return a single value, giving you full control over your synthetic data generation.

```python
## some_custom_function.py
from tablefaker import tablefaker
from faker import Faker

fake = Faker()
def get_level():
    return f"level {fake.random_int(1, 5)}"

```
Add get_level function to your yaml file
```yaml
version: 1
config:
  locale: en_US
  python_import:
    - some_custom_function  # local .py file without .py extension in the same folder as the yaml file
tables:
  - table_name: employee
    row_count: 5
    columns:
      - column_name: id
        data: row_id
      - column_name: person_id
        data: fake.random_int(1, 10)
      - column_name: hire_date
        data: fake.date_between()
      - column_name: level
        data: some_custom_function.get_level() # custom function
```
### TODO
- Add support for full semantic model generation (Snowflake compatible)
- Add support for inserting data into a database directly using dlt
- Add support for updating existing data
- Add support for continuous data generation (streaming)


