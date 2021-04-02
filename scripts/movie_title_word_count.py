import time
# start the clock
start = time.time()

import findspark
findspark.init()
findspark.find()

import collections
import re
import os
import numpy as np
import nltk
from nltk.corpus import stopwords
from pyspark import SparkConf, SparkContext
sw = stopwords.words("english")


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext.getOrCreate(conf = conf)

data_folder = os.getcwd() + '/Documents/UvA/Big Data/uva-bigdata-course-2021-students/src/main/data/'


# helper functions
def titleTokeniser(title):
    title = re.sub(r'"(\d+),(\d+)"', r'\1:\2', title)
    title = title.split(',')
    title = str(title[1])
    title = ''.join([i for i in title if not i.isdigit()])
    title = title.lower().split()
    title = [re.sub(r'\W+', '', word) for word in title if not word in sw] 
    title = list(filter(None, title))
    return title

# start the clock
#start = time.time()

# read movies
lines = sc.textFile(data_folder + "ml-latest/movies-synthetic.csv")
# apply tokensier to create RDD of words
words = lines.flatMap(titleTokeniser)
# reduce by word and count values
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
# sort the results by count
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)
# collect results + print
results = wordCountsSorted.collect()

for result in results[0:15]:
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(result)

# stop clock
end = time.time()
print("Time taken in seconds: " + str(end-start) + ' s')