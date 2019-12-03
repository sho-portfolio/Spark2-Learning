from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.format("csv").option("header", "false").load("ml-100k/u.data")




df.show()

df.printSchema()


df.createOrReplaceTempView("mydataset")
sqlDF = spark.sql("SELECT * FROM mydataset")
sqlDF.show()

from pyspark.sql import sqlContext
df2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load("ml-100k/u.item")
