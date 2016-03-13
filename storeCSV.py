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
####Checks every cell for NULL
def FileStoreSlow(file):
	fileName = file.rstrip('.csv')
	fileName = fileName.rstrip('.CSV')
	fileName = fileName.lstrip('/home/cjnitta/ecs165a/')
	
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
	

		#Make full string
		insertQuery = "insert into %s (%s) values (%s);" % (fileName, stringOfAttr, Sstrings)	
	
		#Gets the values of each tuple
		for row in mkWh:
			parsedRow = []
			for cell in row:
				newCell = parse_value(cell)
				parsedRow.append(newCell)
			cur.execute(insertQuery,tuple(parsedRow))

	print 'Successfully inserted into %s!' %fileName

	
##Does not look at every cell 
def FileStoreFast(file):
	fileName = file.rstrip('.csv')
	fileName = fileName.rstrip('.CSV')
	fileName = fileName.lstrip('/home/cjnitta/ecs165a/')
	
	with open(file, 'rb') as f1:
	
		#Initialize Values
		reader = csv.reader(f1)
		listOfTuples = []
		stringOfAttr= ""
		Sstrings = ""
		numOfAttr = 0
		
		#Adds attribute names to a list and counts how many there are
		firstRow = reader.next()
		for cell in firstRow:
			stringOfAttr += cell + ","
			numOfAttr = numOfAttr + 1
		stringOfAttr = stringOfAttr.rstrip(',')

		#Makes %s string
		for _ in range(numOfAttr - 1):
			Sstrings += '%s,'
		Sstrings += '%s'
	
		#Gets the values of each tuple
		for row in reader:
			listOfTuples.append(row)

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
		   VALUE            FLOAT,
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
		   VALUE            FLOAT,
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
		   VALUE            FLOAT,
		   COLUMN_ORDER		CHAR(5),
		   DESCRIPTION		CHAR(200),
		   UNIT       	    CHAR(49));"""
		   
		   
	cur.execute(queryString)
	print "EIA_CO2_Electricity_2015 Table created successfully"

	
def createTableHHV():
	queryString = """
	DROP TABLE IF EXISTS HHV2PUB;	
	CREATE TABLE HHV2PUB
		(		
		CDIVMSAR     CHAR(2),
		CENSUS_D     CHAR(2),
		CENSUS_R     CHAR(2),
		CNTTDHH      CHAR(2),
		DRVRCNT      DECIMAL(10,2),
		FLAG100      CHAR(2),
		HBHTNRNT     CHAR(2),
		HBHUR        CHAR(2),
		HBPPOPDN     CHAR(5),
		HBRESDN      CHAR(5),
		HH_CBSA      CHAR(5),
		HH_HISP      CHAR(2),
		HH_RACE      CHAR(2),
		HHC_MSA      CHAR(4),
		HHFAMINC     CHAR(2),
		HHRELATD     CHAR(2),
		HHRESP       CHAR(2),
		HHSIZE       DECIMAL(10,2),
		HHSTATE      CHAR(2),
		HHSTFIPS     CHAR(2),
		HHVEHCNT     DECIMAL(10,2),
		HOMEOWN      CHAR(2),
		HOMETYPE     CHAR(2),
		HOUSEID      CHAR(8),
		HTEEMPDN     CHAR(4),
		HTHTNRNT     CHAR(2),
		HTPPOPDN     CHAR(5),
		HTRESDN      CHAR(5),
		LIF_CYC      CHAR(2),
		MSACAT       CHAR(2),
		MSASIZE      CHAR(2),
		NUMADLT      DECIMAL(10,2),
		RAIL         CHAR(2),
		RESP_CNT     DECIMAL(10,2),
		SCRESP       CHAR(2),
		TDAYDATE     CHAR(8),
		TRAVDAY      CHAR(2),
		URBAN        CHAR(2),
		URBANSIZE    CHAR(2),
		URBRUR       CHAR(2),
		VARSTRAT     DECIMAL(10,2),
		WRKCOUNT     DECIMAL(10,2),
		WTHHFIN      DECIMAL(13,5),
		PRIMARY KEY(HOUSEID));"""
		
	cur.execute(queryString)
	print "HHV2PUB Table created successfully"

	
def createTableDAY():
	queryString = """
	DROP TABLE IF EXISTS DAYV2PUB;	
	CREATE TABLE DAYV2PUB(
       AWAYHOME      CHAR(2),
       CDIVMSAR      CHAR(2),
       CENSUS_D      CHAR(2),
       CENSUS_R      CHAR(2),
       DRIVER        CHAR(2),
       DROP_PRK      CHAR(2),
       DRVR_FLG      CHAR(2),
       DRVRCNT       DECIMAL(10,2),
       DWELTIME      DECIMAL(10,2),
       EDUC          CHAR(2),
       ENDTIME       CHAR(4),
       FLAG100       CHAR(2),
       FRSTHM        CHAR(2),
       GASPRICE      DECIMAL(10,2),
       HBHTNRNT      CHAR(2),
       HBHUR         CHAR(2),
       HBPPOPDN      CHAR(5),
       HBRESDN       CHAR(5),
       HH_CBSA       CHAR(5),
       HH_HISP       CHAR(2),
       HH_ONTD       DECIMAL(10,2),
       HH_RACE       CHAR(2),
       HHC_MSA       CHAR(4),
       HHFAMINC      CHAR(2),
       HHMEMDRV      CHAR(2),
       HHRESP        CHAR(2),
       HHSIZE        CHAR(8),
       HHSTATE       CHAR(2),
       HHSTFIPS      CHAR(2),
       HHVEHCNT      DECIMAL(10,2),
       HOMEOWN       CHAR(2),
       HOMETYPE      CHAR(2),
       HOUSEID       CHAR(8),
       HTEEMPDN      CHAR(4),
       HTHTNRNT      CHAR(2),
       HTPPOPDN      CHAR(5),
       HTRESDN       CHAR(5),
       INTSTATE      CHAR(2),
       LIF_CYC       CHAR(2),
       MSACAT        CHAR(2),
       MSASIZE       CHAR(2),
       NONHHCNT      DECIMAL(10,2),
       NUMADLT       DECIMAL(10,2),
       NUMONTRP      DECIMAL(10,2),
       ONTD_P1       CHAR(2),
       ONTD_P10      CHAR(2),
       ONTD_P11      CHAR(2),
       ONTD_P12      CHAR(2),
       ONTD_P13      CHAR(2),
       ONTD_P14      CHAR(2),
       ONTD_P15      CHAR(2),
       ONTD_P2       CHAR(2),
       ONTD_P3       CHAR(2),
       ONTD_P4       CHAR(2),
       ONTD_P5       CHAR(2),
       ONTD_P6       CHAR(2),
       ONTD_P7       CHAR(2),
       ONTD_P8       CHAR(2),
       ONTD_P9       CHAR(2),
       OUTOFTWN      CHAR(2),
       PAYTOLL       CHAR(2),
       PERSONID      CHAR(2),
       PRMACT        CHAR(2),
       PROXY         CHAR(2),
       PSGR_FLG      CHAR(2),
       PUBTRANS      CHAR(2),
       R_AGE         DECIMAL(6,2),
       R_SEX         CHAR(2),
       RAIL          CHAR(2),
       STRTTIME      CHAR(4),
       TDAYDATE      CHAR(8),
       TDCASEID      CHAR(12),
       TDTRPNUM      CHAR(12),
       TDWKND        CHAR(2),
       TRACC1        CHAR(2),
       TRACC2        CHAR(2),
       TRACC3        CHAR(2),
       TRACC4        CHAR(2),
       TRACC5        CHAR(2),
       TRACCTM       DECIMAL(13,5),
       TRAVDAY       CHAR(2),
       TREGR1        CHAR(2),
       TREGR2        CHAR(2),
       TREGR3        CHAR(2),
       TREGR4        CHAR(2),
       TREGR5        CHAR(2),
       TREGRTM       DECIMAL(10,2),
       TRIPPURP      CHAR(8),
       TRPACCMP      DECIMAL(6,2),
       TRPHHACC      DECIMAL(10,2),
       TRPHHVEH      CHAR(2),
       TRPMILES      DECIMAL(13,5),
       TRPTRANS      CHAR(2),
       TRVL_MIN      DECIMAL(13,5),
       TRVLCMIN      DECIMAL(13,5),
       TRWAITTM      DECIMAL(13,5),
       URBAN         CHAR(2),
       URBANSIZE     CHAR(2),
       URBRUR        CHAR(2),
       USEINTST      CHAR(2),
       USEPUBTR      CHAR(2),
       VEHID         CHAR(2),
       VEHTYPE       CHAR(3),
       VMT_MILE      DECIMAL(13,5),
       WHODROVE      CHAR(2),
       WHYFROM       CHAR(2),
       WHYTO         CHAR(2),
       WHYTRP1S      CHAR(2),
       WHYTRP90      CHAR(2),
       WORKER        CHAR(2),
       WRKCOUNT      DECIMAL(13,5),
       WTTRDFIN      CHAR(15),
       PRIMARY KEY(HOUSEID,PERSONID,TDTRPNUM));"""
		
	cur.execute(queryString)
	print "DAYV2PUB Table created successfully"

	
def createTablePER():
	queryString = """
	DROP TABLE IF EXISTS PERV2PUB;	
	CREATE TABLE PERV2PUB(
		BORNINUS     CHAR(2),
		CARRODE      DECIMAL(10,2),
		CDIVMSAR     CHAR(2),
		CENSUS_D     CHAR(2),
		CENSUS_R     CHAR(2),
		CNTTDTR      DECIMAL(10,2),
		CONDNIGH     CHAR(2),
		CONDPUB      CHAR(2),
		CONDRIDE     CHAR(2),
		CONDRIVE     CHAR(2),
		CONDSPEC     CHAR(2),
		CONDTAX      CHAR(2),
		CONDTRAV     CHAR(2),
		DELIVER      DECIMAL(6,2),
		DIARY        CHAR(2),
		DISTTOSC     CHAR(2),
		DISTTOWK     DECIMAL(10,2),
		DRIVER       CHAR(2),
		DRVRCNT      DECIMAL(10,2),
		DTACDT       CHAR(2),
		DTCONJ       CHAR(2),
		DTCOST       CHAR(2),
		DTRAGE       CHAR(2),
		DTRAN        CHAR(2),
		DTWALK       CHAR(2),
		EDUC         CHAR(2),
		EVERDROV     CHAR(2),
		FLAG100      CHAR(2),
		FLEXTIME     CHAR(2),
		FMSCSIZE     DECIMAL(10,2),
		FRSTHM       CHAR(2),
		FXDWKPL      CHAR(2),
		GCDWORK      DECIMAL(13,5),
		GRADE        CHAR(2),
		GT1JBLWK     CHAR(2),
		HBHTNRNT     CHAR(2),
		HBHUR        CHAR(2),
		HBPPOPDN     CHAR(5),
		HBRESDN      CHAR(5),
		HH_CBSA      CHAR(5),
		HH_HISP      CHAR(2),
		HH_RACE      CHAR(2),
		HHC_MSA      CHAR(4),
		HHFAMINC     CHAR(2),
		HHRESP       CHAR(2),
		HHSIZE       DECIMAL(10,2),
		HHSTATE      CHAR(2),
		HHSTFIPS     CHAR(2),
		HHVEHCNT     DECIMAL(10,2),
		HOMEOWN      CHAR(2),
		HOMETYPE     CHAR(2),
		HOUSEID      CHAR(8),
		HTEEMPDN     CHAR(4),
		HTHTNRNT     CHAR(2),
		HTPPOPDN     CHAR(5),
		HTRESDN      CHAR(5),
		ISSUE        CHAR(2),
		LIF_CYC      CHAR(2),
		LSTTRDAY     DECIMAL(10,2),
		MCUSED       DECIMAL(6,2),
		MEDCOND      CHAR(2),
		MEDCOND6     CHAR(2),
		MOROFTEN     CHAR(2),
		MSACAT       CHAR(2),
		MSASIZE      CHAR(2),
		NBIKETRP     DECIMAL(10,2),
		NUMADLT      DECIMAL(10,2),
		NWALKTRP     DECIMAL(6,2),
		OCCAT        CHAR(2),
		OUTCNTRY     CHAR(2),
		OUTOFTWN     CHAR(2),
		PAYPROF      CHAR(2),
		PERSONID     CHAR(2),
		PRMACT       CHAR(2),
		PROXY        CHAR(2),
		PTUSED       DECIMAL(6,2),
		PURCHASE     DECIMAL(6,2),
		R_AGE        DECIMAL(6,2),
		R_RELAT      CHAR(2),
		R_SEX        CHAR(2),
		RAIL         CHAR(2),
		SAMEPLC      CHAR(2),
		SCHCARE      CHAR(2),
		SCHCRIM      CHAR(2),
		SCHDIST      CHAR(2),
		SCHSPD       CHAR(2),
		SCHTRAF      CHAR(2),
		SCHTRN1      CHAR(2),
		SCHTRN2      CHAR(2),
		SCHTYP       CHAR(2),
		SCHWTHR      CHAR(2),
		SELF_EMP     CHAR(2),
		SFWGT        DECIMAL(13,5),
		TDAYDATE     CHAR(8),
		TIMETOSC     DECIMAL(6,2),
		TIMETOWK     DECIMAL(6,2),
		TOSCSIZE     DECIMAL(6,2),
		TRAVDAY      CHAR(2),
		URBAN        CHAR(2),
		URBANSIZE    CHAR(2),
		URBRUR       CHAR(2),
		USEINTST     CHAR(2),
		USEPUBTR     CHAR(2),
		VARSTRAT     DECIMAL(10,2),
		WEBUSE       CHAR(2),
		WKFMHMXX     DECIMAL(6,2),
		WKFTPT       CHAR(2),
		WKRMHM       CHAR(2),
		WKSTFIPS     CHAR(2),
		WORKER       CHAR(2),
		WRKCOUNT     DECIMAL(10,2),
		WRKTIME      CHAR(8),
		WRKTRANS     CHAR(2),
		WTPERFIN     DECIMAL(13,5),
		YEARMILE     DECIMAL(10,2),
		YRMLCAP      CHAR(2),
		YRTOUS       DECIMAL(10,2),
		PRIMARY KEY(HOUSEID,PERSONID));"""
	cur.execute(queryString)
	print "PERV2PUB Table created successfully"

	
def createTableVEH():
	queryString = """
	DROP TABLE IF EXISTS VEHV2PUB;	
	CREATE TABLE VEHV2PUB
		(		ANNMILES     DECIMAL(13,5),
		BEST_EDT     CHAR(2),
		BEST_FLG     CHAR(2),
		BEST_OUT     CHAR(2),
		BESTMILE     DECIMAL(13,5),
		CDIVMSAR     CHAR(2),
		CENSUS_D     CHAR(2),
		CENSUS_R     CHAR(2),
		DRVRCNT      DECIMAL(10,2),
		EIADMPG      DECIMAL(10,2),
		EPATMPG      DECIMAL(13,5),
		EPATMPGF     CHAR(2),
		FLAG100      CHAR(2),
		FUELTYPE     DECIMAL(10,2),
		GSCOST       DECIMAL(10,2),
		GSTOTCST     DECIMAL(10,2),
		GSYRGAL      DECIMAL(10,2),
		HBHTNRNT     CHAR(2),
		HBHUR        CHAR(2),
		HBPPOPDN     CHAR(5),
		HBRESDN      CHAR(5),
		HH_CBSA      CHAR(5),
		HH_HISP      CHAR(2),
		HH_RACE      CHAR(2),
		HHC_MSA      CHAR(4),
		HHFAMINC     CHAR(2),
		HHSIZE       DECIMAL(10,2),
		HHSTATE      CHAR(2),
		HHSTFIPS     CHAR(2),
		HHVEHCNT     DECIMAL(10,2),
		HOMEOWN      CHAR(2),
		HOMETYPE     CHAR(2),
		HOUSEID      CHAR(8),
		HTEEMPDN     CHAR(4),
		HTHTNRNT     CHAR(2),
		HTPPOPDN     CHAR(5),
		HTRESDN      CHAR(5),
		HYBRID       CHAR(2),
		LIF_CYC      CHAR(2),
		MAKECODE     CHAR(2),
		MODLCODE     CHAR(3),
		MSACAT       CHAR(2),
		MSASIZE      CHAR(2),
		NUMADLT      DECIMAL(10,2),
		OD_READ      DECIMAL(11,5),
		PERSONID     CHAR(2),
		RAIL         CHAR(2),
		TDAYDATE     CHAR(8),
		TRAVDAY      CHAR(2),
		URBAN        CHAR(2),
		URBANSIZE    CHAR(2),
		URBRUR       CHAR(2),
		VEHAGE       DECIMAL(10,2),
		VEHCOMM      CHAR(2),
		VEHID        CHAR(2),
		VEHOWNMO     DECIMAL(13,5),
		VEHTYPE      CHAR(3),
		VEHYEAR      DECIMAL(6,2),
		WHOMAIN      CHAR(2),
		WRKCOUNT     DECIMAL(10,2),
		WTHHFIN      DECIMAL(13,5),
		PRIMARY KEY(HOUSEID,VEHID));"""
	cur.execute(queryString)
	print "VEHV2PUB Table created successfully"


	#######MAIN##########
	
##Open Connection
conn = psycopg2.connect("dbname=postgres host = /home/" + os.environ['USER'] + "/postgres ")
print "Opened database successfully"

#Set Cursor
cur = conn.cursor()

#######Importing a database can take 10+ minutes
#######Comment out areas that you have done already

createTableEIA()

createTableHHV()
createTableDAY()
createTablePER()
createTableVEH()

FileStoreSlow('/home/cjnitta/ecs165a/EIA_MkWh_2015.csv')
FileStoreSlow('/home/cjnitta/ecs165a/EIA_CO2_Transportation_2015.csv')
FileStoreSlow('/home/cjnitta/ecs165a/EIA_CO2_Electricity_2015.csv')

FileStoreFast('/home/cjnitta/ecs165a/HHV2PUB.CSV')
FileStoreFast('/home/cjnitta/ecs165a/DAYV2PUB.CSV')
FileStoreFast('/home/cjnitta/ecs165a/VEHV2PUB.CSV')
FileStoreFast('/home/cjnitta/ecs165a/PERV2PUB.CSV')



	
conn.commit()
print "Records created successfully";
conn.close()
