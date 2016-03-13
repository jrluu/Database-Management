#!/usr/bin/python

import ast
import psycopg2
import os 
import csv

#Connect to server
conn = psycopg2.connect("dbname=postgres host = /home/" + os.environ['USER'] + "/postgres ")
print "Opened database successfully"

#Set Cursor
cur = conn.cursor()

daysInMonth = [31,28,31,30,31,30,31,31,30,31,30,31]
month = [200803, 200804,200805,200806,200807,200808,200809,200810,200811,200812,200901,200902,200903,200904]


for date in month:	

	#Get Gallons for each month
	queryString = """			  
			SELECT SUM(TRPMILES/EPATMPG), COUNT(DISTINCT HOUSEID)
			FROM NATURALJOIN
            WHERE CAST(VEHID AS INT) >= 1 AND 
            DRIVER = '01' AND TRPMILES >= 0 AND CAST(TDAYDATE AS INT) = %d; 
		    """ % date
	
	cur.execute(queryString)
	GalAndHHID = cur.fetchone()
	gallon = GalAndHHID[0]
	count = GalAndHHID[1]	
	
	
#	#Get the CO2 Emissions each month
	queryString = """
				SELECT SUM(value) 
				FROM eia_co2_transportation_2015 
				WHERE MSN = 'TEACEUS' AND YYYYMM =  %d; 
				"""  % date
	
	cur.execute(queryString)			
	CO2Emission = cur.fetchone()

	#Get the right month for date
	monthIndex = date%100 - 1
	
	#Convert to the right units
	CO2Emission = CO2Emission[0] * 1000000
	
	resultFor3c = (float(gallon) * 0.00887)	* daysInMonth[monthIndex]	* (117538000/count)	
	resultFor3c = resultFor3c /  float(CO2Emission) * 100
	print "For %s, the percent of transportation C02 Emissions is %s" %(date,resultFor3c) + "%"


conn.commit()
print "postgres closed"
conn.close()
