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
sw = stopwords.words("english")

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

data_folder = '/Users/benplatten/workspace/UvA_BD/bigdata/data/'


# helper functions
def titleTokeniser(title):
    title = title.split(';') 
    title = str(title[1])
    title = ''.join([i for i in title if not i.isdigit()])
    title = title.lower().split()
    title = [re.sub(r'\W+', '', word) for word in title if not word in sw] 
    title = list(filter(None, title))
    return title

# start the clock
start = time.time()

lines = sc.textFile(data_folder + "ml-latest-small/movies.csv")
words = lines.flatMap(titleTokeniser)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)

results = wordCountsSorted.collect()

for result in results[0:15]:
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(result)

# stop clock
end = time.time()
print("Time taken in seconds: " + str(end-start) + ' s')


