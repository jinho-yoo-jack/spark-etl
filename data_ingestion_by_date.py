from pyspark.sql import SparkSession, Row
import tenseal as ts
import pandas as pd

spark = SparkSession \
    .builder \
    .master("local") \
    .appName('rdb2hdfs') \
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
sql = '(select * from bike_trip bt where "startdate" >= '
sql += "'2015-02-02 00:00:00'"
sql += 'and "startdate" <= '
sql += "'2015-02-02 23:59:59') temp"

df = spark.read \
    .format('jdbc') \
    .option('driver', jdbcDriver) \
    .option('url', jdbcUrl) \
    .option('user', rdbUser) \
    .option('password', password) \
    .option('dbtable', sql) \
    .option("timestampFormat", "yyyy-mm-dd hh:mm:ss") \
    .option('partitionColumn', 'startdate') \
    .option('numPartitions', 20) \
    .option('lowerBound', '2015-02-02 00:00:00') \
    .option('upperBound', '2015-02-02 23:59:59') \
    .load()

df.show(10)
rowsCount = df.count()
print(f"Row Count ::: {rowsCount}")