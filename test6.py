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





df2 = spark.read.format("csv").option("header", "false").option("delimiter", '|').load("ml-100k/u.item")
df2.show()

df2.createOrReplaceTempView("mydataset2")
sqlDF2 = spark.sql("SELECT * FROM mydataset2")
sqlDF2.show()
