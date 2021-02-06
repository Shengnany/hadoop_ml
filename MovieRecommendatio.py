# Recommend movies for given user
# Predict rating for every mvovie for every user based on other stuff they watch



from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.ml.recommendation import ALS # recommendation algorithm


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
    return Row(userID = int(fields[0]), movieID = int(fields[1]), rating = (float(fields[2]),1.0))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("MovieRecs").getOrCreate()

    movieNames = loadMovieNames()

    # get the raw data
    # read.txt returns a dataframe
    lines = spark.read.txt('hdfs:///user/maria_dev/u.data').rdd

    # convert it to a rdd of row objects with (movieid,rating)
    movies = lines.map(parseInput)

    #convert it to a dataframe
    # call cache because we actually want to use that data more than once
    # so spark will not try to recreate that more than once
    movieDataset = spark.createDataFrame(movies).cache()

    # ALS is an idea of train/test, predict the model and then train on the data
    als = ALS(maxIter=5, regParam=0.01,userCol="userID",itemCol="movieID",ratingCol='rating')
    model = als.fit(ratings)

    print("\nRatings for user ID 0: ")
    userRatings = ratings.filter("userID = 0")
    for rating in userRatings.collect():
        print(movieNames[rating['movieID']],rating['rating'])

    print("\nTop 20 recommendations: ")
    # Find movies rated more than 100 times
    ratingCounts = ratings.groupBy('movieID').count().filter("count>100")
    # pretend user0 to rate these movies
    popularMovies = ratingCounts.select("movieID").withColumns('userID',lit(0))

    recommendations = model.transform(popularMovies)
    topRecommendations = recommendations.sort(recommendations.prediction.desc()).take(20)
    # compute average rating for each movieid
    averageRatings = movieDataset.groupBy("movieID").avg("rating")
    # cimpute count for each movieid
    counts = movieDataset.groupBy("movieID").count()
    # join the two togetherverageRatings, "movieID")
    # pull the top 10 results
    topTen = averageAndCounts.orderBy("avg(rating)").take(10)
    # pring them out, converting movieID to names
    for recommendation in topRecommendations:
        print(movieNames[recommendations['movieID']],recommendations['prediction')

    spark.stop()