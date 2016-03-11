#!/usr/bin/python

import ast
import psycopg2
import os 
import csv

def calcDays(cur,trip_mile):

	daysInMonth = {"01":31.0,"02":28.0,"03":31.0,"04":30.0,"05":30.0,"06":31.0,"07":30.0,"08":31.0,"09":30.0,"10":31.0,"11":30.0,"12":31.0}

	queryString = r"""
		SELECT RIGHT(tdaydate,2), TRPMILES, EPATMPG
		FROM NATURALJOIN  
		WHERE trpmiles > 0
		GROUP BY personid, houseid, TDTRPNUM
		HAVING SUM(trpmiles) < %s;
		""" % (trip_mile)
	cur.execute(queryString)	
	result = cur.fetchall()
	total = 0.0
	for row in result:
		total += (daysInMonth[row[0]] * float(row[1]) * float(row[2]))
	print total
	return total
	
def createTempVehTable(cur):
	cur.execute('''
		DROP TABLE IF EXISTS VEHTEMP;
		CREATE TABLE VEHTEMP(
		HOUSEID         CHAR(8),
		VEHID           CHAR(2), 
		EPATMPG         DECIMAL(13, 5),    
		PRIMARY KEY(HOUSEID, VEHID));''')

	cur.execute("""INSERT INTO VEHTEMP(HOUSEID, VEHID, EPATMPG)  
			SELECT HOUSEID, VEHID, EPATMPG FROM VEHV2PUB;""");			


def createTempDayTable(cur):
	cur.execute('''
		DROP TABLE IF EXISTS DAYTEMP;
		CREATE TABLE DAYTEMP(
        HOUSEID         CHAR(8),
        PERSONID        CHAR(2),
        TDTRPNUM        CHAR(12),
        TDAYDATE        CHAR(8),
        TRPMILES        DECIMAL(13, 5), 
        VEHID           CHAR(2), 
        PRIMARY KEY(HOUSEID, PERSONID, TDTRPNUM));'''
	)
	cur.execute("""INSERT INTO DAYTEMP(HOUSEID, PERSONID, TDTRPNUM, TDAYDATE, TRPMILES, VEHID) 
              SELECT HOUSEID, PERSONID, TDTRPNUM, CAST(TDAYDATE AS INT), TRPMILES, VEHID FROM DAYV2PUB;""")
	
	
	
def createNaturalJoinTable(cur):
	cur.execute('''
		DROP TABLE IF EXISTS NATURALJOIN;
		CREATE TABLE NATURALJOIN(
        HOUSEID         CHAR(8),
        PERSONID        CHAR(2),
        VEHID           CHAR(2), 
        TDTRPNUM        CHAR(12),
        TRPMILES        DECIMAL(13, 5), 
        EPATMPG         DECIMAL(13, 5),    
        TDAYDATE        CHAR(8),  
        PRIMARY KEY(HOUSEID, PERSONID, TDTRPNUM));'''
	)
	
	cur.execute("""
		INSERT INTO NATURALJOIN(HOUSEID, PERSONID, VEHID, TDTRPNUM, TRPMILES, EPATMPG, TDAYDATE)
		SELECT HOUSEID, PERSONID, VEHID, TDTRPNUM, TRPMILES, EPATMPG, TDAYDATE
		FROM VEHTEMP 
		NATURAL JOIN DAYTEMP;""")	
	
	
def dropTables(cur):
	cur.execute('''
		DROP TABLE IF EXISTS VEHTEMP;
		DROP TABLE IF EXISTS DAYTEMP;
		DROP TABLE IF EXISTS NATURALJOIN;
		''')
		


def main():
	daysInMonth = {"01":31,"02":28,"03":31,"04":30,"05":30,"06":31,"07":30,"08":31,"09":30,"10":31,"11":30,"12":31}
	miles = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100]


	#Calculate the percent of individuals that travel less than 5 -100 miles a day for every 5 mile increments (e.g. 5, 10, 15, ..., 95, 100)
	conn = psycopg2.connect("dbname=postgres host = /home/" + os.environ['USER'] + "/postgres ")
	print "Opened database successfully"

	#Set Cursor
	cur = conn.cursor()
	dropTables(cur)
	
	print "creating tables"
	createTempVehTable(cur)
	createTempDayTable(cur)
	createNaturalJoinTable(cur)
	
	print "ok"
	
	
	for trip_mile in miles:
		total_each_mile = calcDays(cur,trip_mile)
#	result = cur.fetchall()
#	print result
#	dropTables(cur)

	conn.commit()
	print "postgres closed"
	conn.close()

if __name__=='__main__':
	main()