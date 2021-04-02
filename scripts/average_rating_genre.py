import time
# start the clock
start = time.time()

import findspark
findspark.init()
findspark.find()

from pyspark import SparkConf, SparkContext
import collections
import re
import os

conf = SparkConf().setMaster("local").setAppName("AverageRating")
sc = SparkContext.getOrCreate(conf = conf.set("spark.driver.maxResultSize", "4g"))

data_folder = os.getcwd() + '/Documents/UvA/Big Data/code/uva-bigdata-course-2021-students/src/main/data/'


# helper functions
def loadMovieGenres():
    movieGenres = {}
    with open(data_folder + "ml-latest/movies.csv", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = re.sub(r'(?!(([^"]*"){2})*[^"]*$),', '', line)
            fields = fields.split(',')
            genres = fields[2]
            genres = genres.strip('\n')
            genres = genres.split("|")
            movieGenres[int(fields[0])] = genres
    return movieGenres

def parseRating(line):
    fields = line.split(',')
    movieId = int(fields[1])
    rating = float(fields[2])
    return (movieId, rating)

def broadcastJoin(movieId,rating):
    return (movieId,broadcast_id_genre.value[movieId],rating)


# read movies 
rating_lines = sc.textFile(data_folder + "/ml-latest/ratings.csv")
# broadcast ID + genre
broadcast_id_genre = sc.broadcast(loadMovieGenres())
# parse reviews into RDD
ratings_rdd = rating_lines.map(parseRating)
#broadcast join genres with ratings RDD
id_genres_rating = ratings_rdd.map(lambda x: broadcastJoin(x[0],x[1]))
# flatten genre lists into seperate rows
genre_ratings = id_genres_rating.flatMap(lambda x: [(w, x[2]) for w in x[1]]).collect()
# convert back to RDD
g_rdd = sc.parallelize(genre_ratings, numSlices=1000)
# reduce
ratingsByGenre = g_rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x, y: (x[0]+y[0],x[1]+y[1]))
# calculate average
averagesByGenre = ratingsByGenre.mapValues(lambda x: x[0] / x[1])
# collect results + print
results = averagesByGenre.collect()
for result in results:
    print(result)
# stop clock
end = time.time()
print("Time taken in seconds: " + str(end-start) + ' s')


