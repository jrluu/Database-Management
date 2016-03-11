#!/usr/bin/python

import ast
import psycopg2
import os 
import csv

'''
Author:

Problem:
Calculate the percent of individuals that travel less than 5 -100 miles a day for every 5 mile increments (e.g. 5, 10, 15, ..., 95, 100)
'''


def calcDays(cur,trip_mile):

	daysInMonth = {"01":31,"02":28,"03":31,"04":30,"05":30,"06":31,"07":30,"08":31,"09":30,"10":31,"11":30,"12":31}

	queryString = r"""
		SELECT RIGHT(tdaydate,2)
		FROM dayv2pub  
		WHERE trpmiles > 0
		GROUP BY personid, tdaydate, houseid
		HAVING SUM(trpmiles) < %s;
		""" % (trip_mile)
	cur.execute(queryString)	
	result = cur.fetchall()
	total = 0
	for row in result:
		total += daysInMonth[(row[0])]
	return total

	
def calcTotal(cur):

	daysInMonth = {"01":31,"02":28,"03":31,"04":30,"05":30,"06":31,"07":30,"08":31,"09":30,"10":31,"11":30,"12":31}


	queryString = r"""
		SELECT RIGHT(tdaydate,2)
		FROM dayv2pub
		WHERE trpmiles >= 0 
		GROUP BY personid, tdaydate, houseid;"""
		
	cur.execute(queryString)	
	result = cur.fetchall()
	total = 0

	for row in result:
		total += daysInMonth[(row[0])]
		
	return total


def main():

	miles = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100]

	conn = psycopg2.connect("dbname=postgres host = /home/" + os.environ['USER'] + "/postgres ")
	print "Opened database successfully"

	cur= conn.cursor()
	
	total = calcTotal(cur)

	for trip_mile in miles:
		total_each_mile = calcDays(cur, trip_mile)
		final_result = float(total_each_mile)/float(total) * 100.0 
		print str(final_result) + "%" + " travel < %s miles"% trip_mile


	conn.commit()
	print "postgres closed"

	conn.close()

if __name__ == '__main__': 
    main()
	