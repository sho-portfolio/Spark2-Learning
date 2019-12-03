from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions


if __name__ == "__main__":
    # Create a SparkSession (the config bit is only for Windows!)
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

    # Load up our movie ID -> name dictionary
    movieNames = loadMovieNames()

    # Get the raw data
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
    # Convert it to a RDD of Row objects with (movieID, rating)
    movies = lines.map(parseInput)
    # Convert that to a DataFrame
    movieDataset = spark.createDataFrame(movies)

    #sc.textFile("ml-100k/u.data") \
    #    .map(lambda line: line.split(",")) \
    #    .filter(lambda line: len(line)>1) \
    #    .map(lambda line: (line[0],line[1])) \
    #    .collect()    

    df = spark.read.format("csv").option("header", "false").load("ml-100k/u.data")

    # Stop the session
    spark.stop()
