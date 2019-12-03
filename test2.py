from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions


if __name__ == "__main__":
   
    #sc.textFile("ml-100k/u.data") \
    #    .map(lambda line: line.split(",")) \
    #    .filter(lambda line: len(line)>1) \
    #    .map(lambda line: (line[0],line[1])) \
    #    .collect()    

    df = spark.read.format("csv").option("header", "false").load("ml-100k/u.data")

    # Stop the session
    spark.stop()
