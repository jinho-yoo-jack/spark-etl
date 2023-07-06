from pyspark.sql import SparkSession

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
df = spark.read \
    .format('jdbc') \
    .option('driver', jdbcDriver) \
    .option('url', jdbcUrl) \
    .option('user', rdbUser) \
    .option('password', password) \
    .option('dbtable', 'INVENTORIES') \
    .option('partitionColumn', 'WAREHOUSE_ID') \
    .option('lowerBound', 1) \
    .option('upperBound', 10) \
    .option('numPartitions', 3) \
    .load()

df.printSchema()

# Filtering
query = 'SELECT * FROM tempInventories WHERE WAREHOUSE_ID < 7'
dfTempView = df.createTempView('tempInventories')

# Save as parquet at HDFS
spark.sql(query).write.parquet("inventories.parquet")

