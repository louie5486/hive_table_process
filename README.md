# hive_table_process
use this script to transfer hive text table into hive parquet table

1. get original table schema and self parser to get raw columns、partition column.

2. create temp table like with original table.

3. parser temp table to get 'date' column type, replace date column type to string column type.

4. create parquet format table like with temp table.

5. insert data to parquet table from original table.

