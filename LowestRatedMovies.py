# PySpark is the Python API for Spark.
# pyspark.sql module
# pyspark.streaming module
# pyspark.ml package
# pyspark.mllib package
# pyspark.resource module
from pyspark import SparkConf,SparkContext

# data in our local disk
def  loadMovieNames():
    movieNames = {}
    with open("u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# take each line of u.data and convert it 
def parseInput(line):
    fields = line.split()
    return (int(fields[1]), (float(fields[2]),1.0))

if __name__ == "__main__":
    # the main script - create ur SparkContext
    # you can set how it distributed, what cluster it runs on, how much memory is allocated to each executor
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf = conf)
    # load up our movieid -> moviename lookup table
    movieNames = loadMovieNames()
    # look up the raw u.data file and ready to spread across, each row contains a userif, a movieid,a rating and a timestamp
    lines = sc.textfile('hdfs:///user/maria_dev/ml-100k/u.data')
    
    # RDD abstract a lot: you do not have to think about how your data is spread across HDFS 
    
    # convert to (movieID, (rating, 1.0)), RDD.map()
    movieRatings =  lines.map(parseInput)

    # reduce to (movieID, (rating, 1.0))
    ratingToTalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: (movie1[0]+movie2[0],movie2[1]+movie2[1]))

    # map to (movieID, averageRating)
    averageRatings = ratingToTalsAndCount.mapValues(lambda totalAndCount: totalAndCount[0]/totals)

    # sort by average rating
    sortedMovies = averageRatings.sortBy(lambda x: x[1])

    # take the top 10 results -> python lists (rdd to python)
    results = sortedMovies.take(10)

    # print them out:
    for result in results:
        print(movieRatings[result[0],result[1]])