
## Generate Yaml File From Avro Schema or Csv
If you have an [avro schema](https://avro.apache.org/docs/++version++/specification/), you can generate a yaml file using avro_to_yaml function.

```python
from tablefaker import tablefaker
tablefaker.avro_to_yaml("tests/test_person.avsc", "tests/exports/person.yaml")
```

And also you can use csv to define your columns and generate the yaml file.

```python
from tablefaker import tablefaker
tablefaker.csv_to_yaml("tests/test_person.csv", "tests/exports/person.yaml")
```

Sample Csv file
```
column_name,description,data,type,null_percentage
id,Unique identifier for the person,row_id,,
first_name,First name of the person,fake.first_name(),string,
last_name,Last name of the person,fake.last_name(),string,
age,Age of the person,fake.random_int(),int32,0.1
email,Email address of the person,fake.email(),string,0.1
is_active,Indicates if the person is active,fake.pybool(),boolean,0.2
signup_date,Date when the person signed up,fake.date(),,0.3
```
