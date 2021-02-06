from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
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
    return Row(movieID = int(fields[1]), rating = (float(fields[2]),1.0))

if __name__ = "__main__":
    spark = SparkSession.builder.appName("PopularMoviess").getOrCreate()

    movieNames = loadMovieNames()
    # get the raw data
    lines = spark.SparkContext.textFile('hdfs:///user/maria_dev/u.data')
    # convert it to a rdd of row objects with (movieid,rating)
    movies = lines.map(parseInput)
    #convert it to a dataframe
    movieDataset = spark.createDataFrame(movies)
    # compute average rating for each movieid
    averageRatings = movieDataset.groupBy("movieID").avg("rating")
    # cimpute count for each movieid
    counts = movieDataset.groupBy("movieID").count()
    # join the two together
    averageAndCounts = counts.join(averageRatings, "movieID")
    # pull the top 10 results
    topTen = averageAndCounts.orderBy("avg(rating)").take(10)
    # pring them out, converting movieID to names
    for movie in topTen:
        print(movieNames[movie[0]],movie[1],movie[2])

    spark.stop()
