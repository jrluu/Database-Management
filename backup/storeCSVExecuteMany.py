#!/usr/bin/python

import ast
import psycopg2
import os 
import csv

def parse_value(value):
	try:
		# Interpret the string as a Python literal
		return ast.literal_eval(value)
	except Exception:
		# If that doesn't work, assume it's an unquoted string
		if value == 'Not Available':
			value = None
		return value

	
####TAKES IN A Filename, which is a string 
####E.G "blank.csv"
def FILE_OPEN(file):
	fileName = file.rstrip('.csv')
	fileName = fileName.rstrip('.CSV')
	
	with open(file, 'rb') as f1:
	
		#Initialize Values
		mkWh = csv.reader(f1)
		listOfTuples = []
		stringOfAttr= ""
		Sstrings = ""
		numOfAttr = 0
		
		#Adds attribute names to a list and counts how many there are
		firstRow = mkWh.next()
		for cell in firstRow:
			stringOfAttr += cell + ","
			numOfAttr = numOfAttr + 1
		stringOfAttr = stringOfAttr.rstrip(',')

		#Makes %s string
		for _ in range(numOfAttr - 1):
			Sstrings += '%s,'
		Sstrings += '%s'
	
		#Gets the values of each tuple
		for row in mkWh:
			parsedRow = []
			for cell in row:
				newCell = parse_value(cell)
				parsedRow.append(newCell)
			listOfTuples.append(tuple(parsedRow))

		#Insert into Query
		insertQuery = "INSERT INTO %s" %fileName
		insertQuery+="(%s) VALUES(%s)" % (stringOfAttr, Sstrings)
		number_of_rows = cur.executemany(insertQuery,listOfTuples);	  
		
	print 'Successfully inserted into %s!' %fileName

	
def createTableEIA():
	queryString = """
	DROP TABLE IF EXISTS EIA_MkWh_2015;
	CREATE TABLE EIA_MkWh_2015
		   (MSN				CHAR(50),
		   YYYYMM           INT,
		   VALUE            CHAR(50),
		   COLUMN_ORDER		CHAR(5),
		   DESCRIPTION		CHAR(80),
		   UNIT       	    CHAR(49));"""
		   
	cur.execute(queryString)
	print "EIA_MkWh_2015 Table created successfully"

	queryString = r"""
	DROP TABLE IF EXISTS EIA_CO2_Transportation_2015;
	CREATE TABLE EIA_CO2_Transportation_2015
		   (MSN				CHAR(50),
		   YYYYMM           INT,
		   VALUE            CHAR(50),
		   COLUMN_ORDER		CHAR(5),
		   DESCRIPTION		CHAR(200),
		   UNIT       	    CHAR(49));"""

	cur.execute(queryString)
	print "EIA_CO2_Transportation_2015 Table created successfully"

	queryString = r"""
	DROP TABLE IF EXISTS EIA_CO2_Electricity_2015;
	CREATE TABLE EIA_CO2_Electricity_2015
		   (MSN				CHAR(50),
		   YYYYMM           INT,
		   VALUE            CHAR(50),
		   COLUMN_ORDER		CHAR(5),
		   DESCRIPTION		CHAR(200),
		   UNIT       	    CHAR(49));"""
		   
		   
	cur.execute(queryString)
	print "EIA_CO2_Electricity_2015 Table created successfully"

	
def createTableHHV():
	queryString = """
	DROP TABLE IF EXISTS HHV2PUB;	
	CREATE TABLE HHV2PUB
		(HOUSEID VARCHAR(8),
		VARSTRAT INT,
		WTHHFIN INT,
		DRVRCNT INT,
		CDIVMSAR VARCHAR(2),
		CENSUS_D VARCHAR(2),
		CENSUS_R VARCHAR(2),
		HH_HISP VARCHAR(2),
		HH_RACE VARCHAR(2),
		HHFAMINC VARCHAR(2),
		HHRELATD VARCHAR(2),
		HHRESP VARCHAR(2),
		HHSIZE INT,
		HHSTATE VARCHAR(2),
		HHSTFIPS VARCHAR(2),
		HHVEHCNT INT,
		HOMEOWN VARCHAR(2),
		HOMETYPE VARCHAR(2),
		MSACAT VARCHAR(2),
		MSASIZE VARCHAR(2),
		NUMADLT INT,
		RAIL VARCHAR(2),
		RESP_CNT INT,
		SCRESP VARCHAR(2),
		TRAVDAY VARCHAR(2),
		URBAN VARCHAR(2),
		URBANSIZE VARCHAR(2),
		URBRUR VARCHAR(2),
		WRKCOUNT INT,
		TDAYDATE VARCHAR(8),
		FLAG100 VARCHAR(2),
		LIF_CYC VARCHAR(2),
		CNTTDHH VARCHAR(2),
		HBHUR VARCHAR(2),
		HTRESDN VARCHAR(5),
		HTHTNRNT VARCHAR(2),
		HTPPOPDN VARCHAR(5),
		HTEEMPDN VARCHAR(4),
		HBRESDN VARCHAR(5),
		HBHTNRNT VARCHAR(2),
		HBPPOPDN VARCHAR(5),
		HH_CBSA VARCHAR(5),
		HHC_MSA VARCHAR(4));"""
		
	cur.execute(queryString)
	print "DAYV2PUB Table created successfully"

	
def createTableDAY():
	queryString = """
	DROP TABLE IF EXISTS DAYV2PUB;	
	CREATE TABLE DAYV2PUB(
		HOUSEID VARCHAR(8),
		PERSONID VARCHAR(2),
		FRSTHM VARCHAR(2),
		OUTOFTWN VARCHAR(2),
		ONTD_P1 VARCHAR(2),
		ONTD_P2 VARCHAR(2),
		ONTD_P3 VARCHAR(2),
		ONTD_P4 VARCHAR(2),
		ONTD_P5 VARCHAR(2),
		ONTD_P6 VARCHAR(2),
		ONTD_P7 VARCHAR(2),
		ONTD_P8 VARCHAR(2),
		ONTD_P9 VARCHAR(2),
		ONTD_P10 VARCHAR(2),
		ONTD_P11 VARCHAR(2),
		ONTD_P12 VARCHAR(2),
		ONTD_P13 VARCHAR(2),
		ONTD_P14 VARCHAR(2),
		ONTD_P15 VARCHAR(2),
		TDCASEID VARCHAR(12),
		HH_HISP VARCHAR(2),
		HH_RACE VARCHAR(2),
		DRIVER VARCHAR(2),
		R_SEX VARCHAR(2),
		WORKER VARCHAR(2),
		DRVRCNT INT,
		HHFAMINC VARCHAR(2),
		HHSIZE INT,
		HHVEHCNT INT,
		NUMADLT INT,
		FLAG100 VARCHAR(2),
		LIF_CYC VARCHAR(2),
		TRIPPURP VARCHAR(8),
		AWAYHOME VARCHAR(2),
		CDIVMSAR VARCHAR(2),
		CENSUS_D VARCHAR(2),
		CENSUS_R VARCHAR(2),
		DROP_PRK VARCHAR(2),
		DRVR_FLG VARCHAR(2),
		EDUC VARCHAR(2),
		ENDTIME VARCHAR(4),
		HH_ONTD INT,
		HHMEMDRV VARCHAR(2),
		HHRESP VARCHAR(2),
		HHSTATE VARCHAR(2),
		HHSTFIPS VARCHAR(2),
		INTSTATE VARCHAR(2),
		MSACAT VARCHAR(2),
		MSASIZE VARCHAR(2),
		NONHHCNT INT,
		NUMONTRP INT,
		PAYTOLL VARCHAR(2),
		PRMACT VARCHAR(2),
		PROXY VARCHAR(2),
		PSGR_FLG VARCHAR(2),
		R_AGE INT,
		RAIL VARCHAR(2),
		STRTTIME VARCHAR(4),
		TRACC1 VARCHAR(2),
		TRACC2 VARCHAR(2),
		TRACC3 VARCHAR(2),
		TRACC4 VARCHAR(2),
		TRACC5 VARCHAR(2),
		TRACCTM INT,
		TRAVDAY VARCHAR(2),
		TREGR1 VARCHAR(2),
		TREGR2 VARCHAR(2),
		TREGR3 VARCHAR(2),
		TREGR4 VARCHAR(2),
		TREGR5 VARCHAR(2),
		TREGRTM INT,
		TRPACCMP INT,
		TRPHHACC INT,
		TRPHHVEH VARCHAR(2),
		TRPTRANS VARCHAR(2),
		TRVL_MIN INT,
		TRVLCMIN INT,
		TRWAITTM INT,
		URBAN VARCHAR(2),
		URBANSIZE VARCHAR(2),
		URBRUR VARCHAR(2),
		USEINTST VARCHAR(2),
		USEPUBTR VARCHAR(2),
		VEHID VARCHAR(2),
		WHODROVE VARCHAR(2),
		WHYFROM VARCHAR(2),
		WHYTO VARCHAR(2),
		WHYTRP1S VARCHAR(2),
		WRKCOUNT INT,
		DWELTIME INT,
		WHYTRP90 VARCHAR(2),
		TDTRPNUM VARCHAR(12),
		TDWKND VARCHAR(2),
		TDAYDATE VARCHAR(8),
		TRPMILES INT,
		WTTRDFIN INT,
		VMT_MILE INT,
		PUBTRANS VARCHAR(2),
		HOMEOWN VARCHAR(2),
		HOMETYPE VARCHAR(2),
		HBHUR VARCHAR(2),
		HTRESDN VARCHAR(5),
		HTHTNRNT VARCHAR(2),
		HTPPOPDN VARCHAR(5),
		HTEEMPDN VARCHAR(4),
		HBRESDN VARCHAR(5),
		HBHTNRNT VARCHAR(2),
		HBPPOPDN VARCHAR(5),
		GASPRICE INT,
		VEHTYPE VARCHAR(3),
		HH_CBSA VARCHAR(5),
		HHC_MSA VARCHAR(4));"""
		
	cur.execute(queryString)
	print "HH2V2PUB Table created successfully"

	
def createTablePER():
	queryString = """
	DROP TABLE IF EXISTS PERV2PUB;	
	CREATE TABLE PERV2PUB
		(HOUSEID VARCHAR(8),
		PERSONID VARCHAR(2),
		VARSTRAT INT,
		WTPERFIN INT,
		SFWGT INT,
		HH_HISP VARCHAR(2),
		HH_RACE VARCHAR(2),
		DRVRCNT INT,
		HHFAMINC VARCHAR(2),
		HHSIZE INT,
		HHVEHCNT INT,
		NUMADLT INT,
		WRKCOUNT INT,
		FLAG100 VARCHAR(2),
		LIF_CYC VARCHAR(2),
		CNTTDTR INT,
		BORNINUS VARCHAR(2),
		CARRODE INT,
		CDIVMSAR VARCHAR(2),
		CENSUS_D VARCHAR(2),
		CENSUS_R VARCHAR(2),
		CONDNIGH VARCHAR(2),
		CONDPUB VARCHAR(2),
		CONDRIDE VARCHAR(2),
		CONDRIVE VARCHAR(2),
		CONDSPEC VARCHAR(2),
		CONDTAX VARCHAR(2),
		CONDTRAV VARCHAR(2),
		DELIVER INT,
		DIARY VARCHAR(2),
		DISTTOSC VARCHAR(2),
		DRIVER VARCHAR(2),
		DTACDT VARCHAR(2),
		DTCONJ VARCHAR(2),
		DTCOST VARCHAR(2),
		DTRAGE VARCHAR(2),
		DTRAN VARCHAR(2),
		DTWALK VARCHAR(2),
		EDUC VARCHAR(2),
		EVERDROV VARCHAR(2),
		FLEXTIME VARCHAR(2),
		FMSCSIZE INT,
		FRSTHM VARCHAR(2),
		FXDWKPL VARCHAR(2),
		GCDWORK INT,
		GRADE VARCHAR(2),
		GT1JBLWK VARCHAR(2),
		HHRESP VARCHAR(2),
		HHSTATE VARCHAR(2),
		HHSTFIPS VARCHAR(2),
		ISSUE VARCHAR(2),
		OCCAT VARCHAR(2),
		LSTTRDAY INT,
		MCUSED INT,
		MEDCOND VARCHAR(2),
		MEDCOND6 VARCHAR(2),
		MOROFTEN VARCHAR(2),
		MSACAT VARCHAR(2),
		MSASIZE VARCHAR(2),
		NBIKETRP INT,
		NWALKTRP INT,
		OUTCNTRY VARCHAR(2),
		OUTOFTWN VARCHAR(2),
		PAYPROF VARCHAR(2),
		PRMACT VARCHAR(2),
		PROXY VARCHAR(2),
		PTUSED INT,
		PURCHASE INT,
		R_AGE INT,
		R_RELAT VARCHAR(2),
		R_SEX VARCHAR(2),
		RAIL VARCHAR(2),
		SAMEPLC VARCHAR(2),
		SCHCARE VARCHAR(2),
		SCHCRIM VARCHAR(2),
		SCHDIST VARCHAR(2),
		SCHSPD VARCHAR(2),
		SCHTRAF VARCHAR(2),
		SCHTRN1 VARCHAR(2),
		SCHTRN2 VARCHAR(2),
		SCHTYP VARCHAR(2),
		SCHWTHR VARCHAR(2),
		SELF_EMP VARCHAR(2),
		TIMETOSC INT,
		TIMETOWK INT,
		TOSCSIZE INT,
		TRAVDAY VARCHAR(2),
		URBAN VARCHAR(2),
		URBANSIZE VARCHAR(2),
		URBRUR VARCHAR(2),
		USEINTST VARCHAR(2),
		USEPUBTR VARCHAR(2),
		WEBUSE VARCHAR(2),
		WKFMHMXX INT,
		WKFTPT VARCHAR(2),
		WKRMHM VARCHAR(2),
		WKSTFIPS VARCHAR(2),
		WORKER VARCHAR(2),
		WRKTIME VARCHAR(8),
		WRKTRANS VARCHAR(2),
		YEARMILE INT,
		YRMLCAP VARCHAR(2),
		YRTOUS INT,
		DISTTOWK INT,
		TDAYDATE VARCHAR(8),
		HOMEOWN VARCHAR(2),
		HOMETYPE VARCHAR(2),
		HBHUR VARCHAR(2),
		HTRESDN VARCHAR(5),
		HTHTNRNT VARCHAR(2),
		HTPPOPDN VARCHAR(5),
		HTEEMPDN VARCHAR(4),
		HBRESDN VARCHAR(5),
		HBHTNRNT VARCHAR(2),
		HBPPOPDN VARCHAR(5),
		HH_CBSA VARCHAR(5),
		HHC_MSA VARCHAR(4));"""
	cur.execute(queryString)
	print "PERV2PUB Table created successfully"

	
def createTableVEH():
	queryString = """
	DROP TABLE IF EXISTS VEHV2PUB;	
	CREATE TABLE VEHV2PUB
		(HOUSEID VARCHAR(8),
		WTHHFIN INT,
		VEHID VARCHAR(2),
		DRVRCNT INT,
		HHFAMINC VARCHAR(2),
		HHSIZE INT,
		HHVEHCNT INT,
		NUMADLT INT,
		FLAG100 VARCHAR(2),
		CDIVMSAR VARCHAR(2),
		CENSUS_D VARCHAR(2),
		CENSUS_R VARCHAR(2),
		HHSTATE VARCHAR(2),
		HHSTFIPS VARCHAR(2),
		HYBRID VARCHAR(2),
		MAKECODE VARCHAR(2),
		MODLCODE VARCHAR(3),
		MSACAT VARCHAR(2),
		MSASIZE VARCHAR(2),
		OD_READ INT,
		RAIL VARCHAR(2),
		TRAVDAY VARCHAR(2),
		URBAN VARCHAR(2),
		URBANSIZE VARCHAR(2),
		URBRUR VARCHAR(2),
		VEHCOMM VARCHAR(2),
		VEHOWNMO INT,
		VEHYEAR INT,
		WHOMAIN VARCHAR(2),
		WRKCOUNT INT,
		TDAYDATE VARCHAR(8),
		VEHAGE INT,
		PERSONID VARCHAR(2),
		HH_HISP VARCHAR(2),
		HH_RACE VARCHAR(2),
		HOMEOWN VARCHAR(2),
		HOMETYPE VARCHAR(2),
		LIF_CYC VARCHAR(2),
		ANNMILES INT,
		HBHUR VARCHAR(2),
		HTRESDN VARCHAR(5),
		HTHTNRNT VARCHAR(2),
		HTPPOPDN VARCHAR(5),
		HTEEMPDN VARCHAR(4),
		HBRESDN VARCHAR(5),
		HBHTNRNT VARCHAR(2),
		HBPPOPDN VARCHAR(5),
		BEST_FLG VARCHAR(2),
		BESTMILE INT,
		BEST_EDT VARCHAR(2),
		BEST_OUT VARCHAR(2),
		FUELTYPE INT,
		GSYRGAL INT,
		GSCOST INT,
		GSTOTCST INT,
		EPATMPG INT,
		EPATMPGF VARCHAR(2),
		EIADMPG INT,
		VEHTYPE VARCHAR(3),
		HH_CBSA VARCHAR(5),
		HHC_MSA VARCHAR(4));"""
	cur.execute(queryString)
	print "VEHV2PUB Table created successfully"

	#######MAIN
##Open Connection
conn = psycopg2.connect("dbname=postgres host = /home/" + os.environ['USER'] + "/postgres ")
print "Opened database successfully"

#Set Cursor
cur = conn.cursor()

#######Importing a database can take 10+ minutes
#######Comment out areas that you have done already

#createTableEIA()

#createTableHHV()
createTableDAY()
#createTablePER()
#createTableVEH()

#FILE_OPEN('EIA_MkWh_2015.csv')
#FILE_OPEN('EIA_CO2_Transportation_2015.csv')
#FILE_OPEN('EIA_CO2_Electricity_2015.csv')

#still need to import day, perv, and veh
#FILE_OPEN('HHV2PUB.CSV')
FILE_OPEN('DAYV2PUB.CSV')
#FILE_OPEN('PERV2PUB.CSV')
#FILE_OPEN('VEHV2PUB.CSV')






	
conn.commit()
print "Records created successfully";
conn.close()
