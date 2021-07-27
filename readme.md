# Automate Table Reprocessing

This reprository consist of an approach to automate the massivley routine for reprocessing tables. All you need to know is that you just need to write a simple configuration file in ```/ymls``` and make available the sql query file in ```/sql``` directory.

The logical consist into just use python to call an script file (it can be writed in python or another programing language since the machine where it will be executed can do it) pre set in a yaml configuration file. In this configuration file also has informations like databse connection, data for retroactive load and others params that need to be considered as well.

For you easily understand, you need to write a configuration file like this:

```yaml
table1:
  script_name: /path/myfolder/myfile1.py
  schema: table_schema
  table_name: table_name
  sql_file: sql/table_sql_file1.sql
  data_field: data_field
  rds_host: localhost
  rds_port: port_number
  rds_database: database_name
  rds_user: username
  rds_password: password
  s3_artifacts_bucket: bucket_name
  source_database_name: database_name_athena
  s3_output_querys: s3_folder_name
  start_date: 01-01-1999
  end_date: 31-12-2050
  
table2:
  script_name: /path/myfolder/myfile2.py
  sql_file: sql/table_sql_file2.sql
  data_field: data_field
  source_database_name: source_database_name_athena
  target_database_name: target_database_name_athena
  s3_artifacts_bucket: bucket_name
  s3_output_querys: s3_folder_name
  start_date: 01-01-1999
  end_date: 31-12-2050
```

and after that easily run your code with command bellow.

```bash
python jobs/main.py
```

Remember that's necessary a sql file into sql directorey with same name typed into "sql_file" into yaml configuration table.