## dbt demo

This repository demonstrates how to use dbt to publish data that adheres to a controlled vocabulary naming convention.

To motivate the example, it pulls from Google BigQuery public datasets to create a table to evaluate the accuracy of a COVID forecasting model.
The author is not an expert on COVID. This repository is very much *not* intended to make any statements about COVID models or to demonstrate
best practices of model monitoring. This is simply an example using a set of publicly available datasets in a database on an understandable and
widely relatable topic.

To understand the overall file structure, see the dbt [documentation](https://docs.getdbt.com/)

Useful resources specific to this project are found in:

- `macros`
  + `get_column_names()`: Returns all column names for a relation as a list
  + `get_matches()`: Returns all elements of a list matching a regex
- `models`
  + `model_monitor.sql`: Demonstrates the use of the above two macros`
- `tests`
  + All custom tests are intended to check for controlled vocabulary "contract" adherence