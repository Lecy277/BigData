from operator import add
from pyspark import SparkConf, SparkContext
import string
import sys
import re
from bs4 import BeautifulSoup


def removePunctuation(text):
	text=text.lower().strip()
	text=re.sub('[^0-9a-zA-Z ]', '', text)
	return text 
	
def wordCount(wordListRDD):
	return wordListRDD.map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b)

def pass_filter(x):
	return (len(x) > 0 or x != ' ' or x != None)

if __name__ == "__main__":
	conf = SparkConf().setAppName("My_Big_Data_Program").set("spark.executor.memory", "10g")
	sc = SparkContext(conf=conf)
	lines = sc.wholeTextFiles("file:///C:/Users/lecy2/Desktop/wpcd/wp/1/*",12) # path to a text file in local file system

	#Run streaming query1 in scheduler pool1
	sc.setLocalProperty("spark.scheduler.pool", "pool1")
	File_Key_rdd=lines.keys()
	File_Value_rdd=lines.values()
	
	counts_Key = File_Key_rdd.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add)

	output_FileKey = (counts_Key.collect())

	primary_linkcount=0
	
	for (word, count) in output_FileKey:
		print "%s: %i" % (word, count)
		primary_linkcount+=count
	print "Main page count = %i" % (primary_linkcount)
	
	#Run streaming query2 in scheduler pool2
	sc.setLocalProperty("spark.scheduler.pool", "pool2")
	counts_Value = File_Value_rdd.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add)
	outputValues = (counts_Value.collect())
	
	redlinkcount=0
	href_count=0
	http_count=0
	https_count=0
	
	for (word, count) in outputValues:
		if word.find("edit&redlink=1") != -1:
			redlinkcount+=count
			
		if word.find("href=") != -1:
			href_count+=count

		if word.find("http://") != -1:
			http_count+=count
			
		if word.find("https://") != -1:
			https_count+=count
	print "Redlink count = %i" % (redlinkcount)
	print "href link count = %i" % (href_count)
	print "http count = %i" % (http_count)
	print "https count = %i" % (https_count)
	
	wikipediaWordsRDD = File_Value_rdd.flatMap(lambda x:x.split(' ')).map(removePunctuation).filter(pass_filter)
	
	#perform a word count to find the top 15 words on wikipedia pages
	output = wordCount(wikipediaWordsRDD).map(lambda (k,v):(v,k)).sortByKey(False).take(15)

	for (word, count) in output:
			print "%i: %s" % (word,count)
	
	#perform a word count to find the number of times numbers are words are used used on wikipedia pages
	numbersRDD = wikipediaWordsRDD.filter(lambda x: x=="zero" or x=="one" or x=="two" or x=="three" or x=="four" or x=="five" or x=="six" or x=="seven" or x=="eight" or x=="nine")
	output1=wordCount(numbersRDD).map(lambda (k,v):(v,k)).sortByKey(False).take(15)
	for (word, count) in output1:
		print "%i: %s" % (word,count)
	
	sc.stop()