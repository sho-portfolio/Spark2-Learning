# https://spark.apache.org/docs/1.6.1/sql-programming-guide.html


from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

from pyspark.sql.types import StructType, StructField, IntegerType
schemaUser = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("date", IntegerType(), True)]
)

df = spark.read.format("csv").option("header", "false").option("schema", "schemaUser").load("ml-100k/u.data")


df.show()

df.printSchema()


df.createOrReplaceTempView("mydataset")
sqlDF = spark.sql("SELECT * FROM mydataset")
sqlDF.show()





df2 = spark.read.format("csv").option("header", "false").option("delimiter", '|').load("ml-100k/u.item")
df2.show()

df2.createOrReplaceTempView("mydataset2")
sqlDF2 = spark.sql("SELECT "_c01" FROM mydataset2")
sqlDF2.show()
