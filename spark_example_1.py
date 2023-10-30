from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .master("local") \
    .appName('csv2rdb') \
    .getOrCreate()

rdbUser = 'OT'
password = 'oracle'
server = 'localhost'
port = 1521
service_name = 'XE'
jdbcUrl = "jdbc:oracle:thin:@localhost:1521:XE"
jdbcDriver = "oracle.jdbc.driver.OracleDriver"

# def getDataFromRdb(rdbURL, rdbUser, rdbPassword):
# jdbc -> HDFS
df = spark.read \
    .option("header", True) \
    .csv("/Users/black/dev/bigdata/data/bike-data/201508_trip_data.csv")
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

dateConvertedDf = df \
    .withColumn("startdate", to_timestamp(col("startdate"), "MM/dd/yyyy HH:mm")) \
    .withColumn("startdate", to_timestamp(col("startdate"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("enddate", to_timestamp(col("enddate"), "MM/dd/yyyy HH:mm")) \
    .withColumn("enddate", to_timestamp(col("enddate"), "yyyy-MM-dd HH:mm:ss"))

dateConvertedDf.write \
    .format('jdbc') \
    .option('driver', jdbcDriver) \
    .option('url', jdbcUrl) \
    .option('user', rdbUser) \
    .option('password', password) \
    .option('dbtable', 'bike_trip') \
    .mode('overwrite') \
    .save()

# df.printSchema()
dateConvertedDf.printSchema()
dateConvertedDf.show(10)
