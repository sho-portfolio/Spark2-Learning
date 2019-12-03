# https://spark.apache.org/docs/1.6.1/sql-programming-guide.html


from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.format("csv").option("header", "false").option("delimiter", "\t").load("ml-100k/u.data")


df.show()

df.printSchema()


df.createOrReplaceTempView("mydataset")
sqlDF = spark.sql("SELECT * FROM mydataset")
sqlDF.show()





df2 = spark.read.format("csv").option("header", "false").option("delimiter", '|').load("ml-100k/u.item")
df2.show()

df2.createOrReplaceTempView("mydataset2")
sqlDF2 = spark.sql("SELECT mydataset2._c0 FROM mydataset2")
sqlDF2.show()

# JOIN ATTEMPT
sqlDFJoin = spark.sql("SELECT * FROM mydataset2 INNER JOIN mydataset ON mydataset._c0 = mydataset2._c0 WHERE mydataset2._c0 < 10")
sqlDFJoin.show()
