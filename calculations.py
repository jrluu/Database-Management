#!/usr/bin/python

import psycopg2
import os

nums = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100]
months = {"01": 31,"02": 28,"03": 31,"04": 30,"05": 31,"06": 30,"07": 31,"08": 31,"09": 30,"10": 31,"11": 30,"12": 31}

conn = psycopg2.connect("dbname=postgres host = /home/" + os.environ['USER'] + "/postgres ")

cur = conn.cursor()

# 3a

cur.execute('''SELECT RIGHT(TDAYDATE,2) FROM DAYV2PUB WHERE TRPMILES > 0 GROUP BY HOUSEID,PERSONID, TDAYDATE;''')
results = cur.fetchall()

totalsum = 0
for result in results:
    totalsum += months[result[0]]

print "totalsum = ", totalsum


for i in nums:
    cur.execute('''SELECT RIGHT(TDAYDATE,2) FROM DAYV2PUB WHERE TRPMILES > 0 GROUP BY HOUSEID,PERSONID, TDAYDATE HAVING SUM(TRPMILES) < '''+str(i)+ ';')
    results = cur.fetchall();

    Sum = 0
    for result in results:
        Sum += months[result[0]]

    print "miles < "+str(i)+ " =    "+str(100.0*float(Sum)/float(totalsum))+"%"



cur.execute('''SELECT RIGHT(TDAYDATE,2),EPATMPG,TRPMILES FROM VEHV2PUB NATURAL JOIN DAYV2PUB WHERE TRPMILES > 0; ''')
results = cur.fetchall()
Sum = 0
denom = 0
for result in results:
    Sum += months[result[0]]*result[1]*result[2]
    denom += months[result[0]] * result[2]

print float(Sum)/float(denom)

#3c

conn.commit()
conn.close()
