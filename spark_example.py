from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master("local") \
    .appName('rdb2hdfs') \
    .getOrCreate()

spark.conf.get

query = 'SELECT * FROM REGIONS'
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
    .option('dbtable', '(select * from INVENTORIES where WAREHOUSE_ID < 7)') \
    .option('partitionColumn', 'WAREHOUSE_ID') \
    .option('lowerBound', 1) \
    .option('upperBound', 10) \
    .option('numPartitions', 3) \
    .load()

df.show()
