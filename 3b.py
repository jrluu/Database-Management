#!/usr/bin/python

import ast
import psycopg2
import os 
import csv

#Calculate the percent of individuals that travel less than 5 -100 miles a day for every 5 mile increments (e.g. 5, 10, 15, ..., 95, 100)
conn = psycopg2.connect("dbname=postgres host = /home/" + os.environ['USER'] + "/postgres ")
print "Opened database successfully"

#Set Cursor
cur = conn.cursor()

#3b
daysInMonth = {"01":31,"02":28,"03":31,"04":30,"05":30,"06":31,"07":30,"08":31,"09":30,"10":31,"11":30,"12":31}
miles = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100]

def calc(trip_mile):

	
	queryString = r"""
		SELECT RIGHT(tdaydate,2), trpmiles, epatmpg
		FROM VEHV2PUB NATURAL JOIN DAYV2PUB 
		WHERE vehid > 0 and trpmiles > 0
		HAVING SUM(trpmiles) < %s;
		""" % (trip_mile)
	return queryString
	
conn = psycopg2.connect("dbname=postgres host = /home/" + os.environ['USER'] + "/postgres ")
print "Opened database successfully"

for trip_mile in miles:
	queryString = calc(trip_mile)
	cur.execute(queryString)	
	result = cur.fetchall()

	total = 0
	for row in result:
		total += float(daysInMonth[(row[0])]) * float(row[1]) * float(row[2])
	print total
	print "total"
	
#Set Cursor
cur = conn.cursor()
conn.commit()
print "postgres closed"
conn.close()
