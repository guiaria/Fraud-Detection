#!/usr/bin/python
import math

from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("reduceSum").setMaster("local[1]")

def sigmadata(line):  # This function use to map and return (key,(value1,value2)) which key = userid , value1 = amount and value2 = amount^2 
	data = line.split(",")
	userid = data[0]
	amount = float(data[1]) # Change csv file datatype from string to int 
	return userid, (amount, pow(amount,2) ,1)

def trainModel(field): # Map from sigmadata to return (userid,mean+sd*2,mean-sd*2)
    userid = field[0]
    value = field[1]
    sumX = value[0]
    number = value[2]
    mean = float(value[0] / number)
    sumXsq = float(value[1])
    sd = math.sqrt((sumXsq-(pow(sumX,2)/number))/number-1)
    sd1 = 2*sd
    return userid , mean - sd1 , mean + sd1

sc = SparkContext(conf=conf)

wordSet = sc.textFile("hdfs:/user/training/csv3,hdfs:/user/training/csv2,hdfs:/user/training/csv1")

csv = wordSet.map(lambda line:sigmadata(line))
csv = csv.reduceByKey(lambda a,b:(a[0]+b[0], a[1]+b[1], a[2]+b[2])) # reduce by key sum amount and amount^2 
csv = csv.map(lambda data:trainModel(data))
csv = csv.map(lambda (k, v1, v2): "{0},{1},{2}".format(k, v1, v2)) # Delete () and "" From file
csv = csv.repartition(1)	# Make output left only 1 file
csv = csv.saveAsTextFile("hdfs:/user/training/finalmodel5")
