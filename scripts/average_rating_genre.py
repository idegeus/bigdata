import findspark
findspark.init()
findspark.find()

from pyspark import SparkConf, SparkContext
import collections
import re
import time

conf = SparkConf().setMaster("local").setAppName("AverageRating")
sc = SparkContext(conf = conf)

data_folder = '/Users/benplatten/workspace/UvA_BD/bigdata/data/'


# helper functions
def loadMovieGenres():
    movieGenres = {}
    with open(data_folder + "ml-latest-small/movies.csv", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split(';')
            genres = fields[2]
            genres = genres.strip('\n')
            genres = genres.split("|")
            movieGenres[int(fields[0])] = genres
    return movieGenres

def parseRating(line):
    fields =line.split(';')
    movieId = int(fields[1])
    rating= float(fields[2])
    return (movieId, rating)

def broadcastJoin(movieId,rating):
    return (movieId,broadcast_id_genre.value[movieId],rating)


# start the clock
start = time.time()

# read movies 
rating_lines = sc.textFile(data_folder+"/ml-latest-small/ratings.csv")
# broadcast ID + genre
broadcast_id_genre = sc.broadcast(loadMovieGenres())
# parse reviews into RDD
ratings_rdd = rating_lines.map(parseRating)
#broadcast join genres with ratings RDD
id_genres_rating = ratings_rdd.map(lambda x: broadcastJoin(x[0],x[1]))
# flatten genre lists into seperate rows
genre_ratings = id_genres_rating.flatMap(lambda x: [(w, x[2]) for w in x[1]]).collect()
# convert back to RDD
g_rdd = sc.parallelize(genre_ratings)
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


