Version 1.0: Take into account for Buy/Sell.

There are two file in this zipped file:
1. mysql_script.sql
2. main.py

The sql script file is used to create a new database schema named 'proj'
The py file is used to automatically create the database and load data into the database.

Before running the main.py, please make sure there is no important data currently stored in the MySQL schema named 'proj'.

All database creating processes are automatically performed by the main.py file, there is no need to manually run the SQL script to create a new schema.

When running the main.py, please provide mysql server host name, user name and password for the automatic process to work. Please also make sure the Customer Name match exactly with the Customer Name provided by iso.ne.com, otherwise the program will fail to extra customer ID from the database.

Please also noted: some of the foreign might fail because the range user choosed might not fully represent the entire custome base. However, a failed foreign key should not affect the program to extra profit results.

After running the program, the over-all profit for a choosen customer on a specific day will be both displayed on the console, and being stored in the database tabel named 'profit'.

