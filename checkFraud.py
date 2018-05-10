#!/usr/bin/python
import socket               # Import socket module
import time
import datetime
import sys
import smtplib
import json
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

conf = SparkConf().setAppName("Frauddetection").setMaster("local[1]")

sc = SparkContext("local[2]", "Fraudcount")
ssc = StreamingContext(sc, 1)

global s

dataSet = sc.textFile("hdfs:/user/training/fraud1.csv")		#change to RDD
getmeanpos = {}												#Initialization
getmeanneg = {}				
for record in dataSet.toLocalIterator():					#For loop	
						
	field = record.split(",")								#Split the record by ,
	userid = field[0]										#set userid as field 0								
	meanpositive2sd = field[1]								#set meanpositive2sd as field 1
	meannegative2sd  = field[2]								#set meannegative2sd as field 0
	getmeanpos[userid] = meanpositive2sd 					
	getmeanneg[userid] = meannegative2sd 	
		
def compare(userid,amount):									#function campare
	if int(amount) > float(getmeanpos.get(userid)):			#if amount > mean+
		msg = "FRAUD userid "+str(userid)+" , "+str(amount)+" , mean"+str(float(getmeanpos.get(userid)))	#set msg equal to fraud detail
		s.send(msg)																							#send msg to client
		email(msg)																							#send email
	elif int(amount) < float(getmeanneg.get(userid)):
		msg = "FRAUD userid "+str(userid)+" , "+str(amount)+" , mean"+str(float(getmeanneg.get(userid)))	#set msg equal to fraud detail
		s.send(msg)																							#send msg to client
		email(msg)																							#send email

def email(msg):
	global server
	server = smtplib.SMTP('smtp.gmail.com',587)
	server.starttls()
	server.login("dummyparallel7235@gmail.com", "potato123")												#Fucnction email 
	gmail_pwd = 'potato123'
	server.sendmail("dummyparallel7235@gmail.com", "dummyparallel7235@gmail.com", msg)			   #Send email to according to given info
	
def sendRecord(inputStreamLines):						   #Func to send to client

	data = inputStreamLines.map(lambda x: json.loads(x)).collect()	# Load batch of JSON file from RDD into data
	for row in data:   									   # Loop to check for each record from JSON file
		compare(row['userid'],row['amount'])			   #compare userid and amount
					
			

inputStreamLines = ssc.socketTextStream("localhost", 6666)	#Create d-stream to connect hostname and port
a = inputStreamLines.foreachRDD(sendRecord)					#let sendrecord to be at each RDD function
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)		#socket initilization
s.connect(('localhost',9999))								#Connect to address


ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
