import findspark
findspark.init()
findspark.find()

from pyspark import SparkConf, SparkContext
import collections
import re
import time
import numpy as np
import nltk
from nltk.corpus import stopwords
nltk.download('stopwords')
from nltk.tokenize import word_tokenize


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

data_folder = '/Users/benplatten/workspace/UvA_BD/bigdata/data/'


# helper functions
def titleTokeniser(title):
    title = ''.join([i for i in title if not i.isdigit()])
    title = [x.strip() for x in title.split()]
    title = [re.sub(r'\W+', '', i) for i in title]
    title = list(filter(None, title))
    title = [word for word in title if not word in stopwords.words()]
    return title
   
def parseLine(line):
    fields = line.split(';') 
    title = str(fields[1]) 
    title = titleTokeniser(title.lower)
    return (title)


# start the clock
start = time.time()

lines = sc.textFile(data_folder + "ml-latest-small/movies.csv")
rdd = lines.flatMap(parseLine)

wordCounts = rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)

results = wordCountsSorted.collect()

for result in results[0:15]:
    print(result)

# stop clock
end = time.time()
print("Time taken in seconds: " + str(end-start) + ' s')


