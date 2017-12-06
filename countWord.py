from __future__ import print_function

import sys
import re
import numpy as np
import shutil
from random import random
from operator import add

from pyspark import SparkContext, SparkConf

def parseLine(line):
	searchObj = re.search( r'(([0-9A-z]+),([0-9]+) ?)+', line)
	if searchObj:
   		return searchObj.group().split(" ")
	else:
		return  ""


if __name__ == "__main__":

	conf = (SparkConf()
         .setMaster("local")
         .setAppName("BRAULT_BOYERE_COUNTWORD")
         .set("spark.executor.memory", "1g"))
	sc = SparkContext(conf = conf)	

	shutil.rmtree('/spark-2.2.0-bin-hadoop2.7/wordsOut')

	text_file = sc.textFile("file:///spark-2.2.0-bin-hadoop2.7/words.txt")
	#text_file = sc.textFile("hdfs://esir-abd-boyere-brault.istic.univ-rennes1.fr:8020/user/hadoop/words.txt")


	counts = text_file.flatMap(parseLine).map(lambda word: (word.split(",")[0], int(word.split(",")[1]))).reduceByKey(lambda a, b: a + 1).sortBy(lambda a: a[1],False)
	#reduceByKey(lambda a, b: a + b) pour la question 1
	text_file.saveAsTextFile("file:///spark-2.2.0-bin-hadoop2.7/wordsOut")
sc.stop()


