#Database-Management

##Summary
This project is a class project to store data and run queries on a PostgreSQL Database.
To communicate to the database, I used the package Psycopg.
These python scripts run on Python 2.7

##File Descriptions
storeCSV.py creates the CSV tables into the database postgres and then inserts over a million tuples from the National Household Travel Survey(NHTS)'s .csv file.

Part 3a of the program is the file called query3a.py and takes around 5 minutes to print out the percent of individuals that travel less than 5-100 miles.

Part 3b is the file called query3b.py and takes around 13 minutes to print out the average fuel economy of all miles traveled for trips. This file creates new tables to query from.

Part 3c needs the tables created from Part 3b file query3b.py because it refers to the same tables for the natural join. This part takes around 10 seconds to print out the values for the percent of transporttion of CO_2 emissions.

#For More Information

* [Python 2.7](https://www.python.org/download/releases/2.7/)
* [PostgreSQL](https://www.postgresql.org/)
* [psycopg]
(http://initd.org/psycopg)


#Contact
jrluu@ucdavis.edu
