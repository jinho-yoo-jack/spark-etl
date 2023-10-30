from pyspark.sql import SparkSession, Row
import tenseal as ts
import pandas as pd

# Key Generation
context = ts.context(ts.SCHEME_TYPE.CKKS, poly_modulus_degree=8192, coeff_mod_bit_sizes=[60, 40, 40, 60])
context.generate_galois_keys()
context.global_scale = 2 ** 40

# 1. generate private and public key pair.
# private key
# secret_context = context.serialize(save_secret_key=True)
# context.make_context_public()
# public key
# public_context = context.serialize()

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

# df.select(col('WAREHOUSE_ID_ENC'), expr(ts.ckks_vector(context, df.select('WAREHOUSE_ID').rdd.flatMap(lambda x: x).collect()))).show(3)
# df.withColumn('WAREHOUSE_ID_ENCRYPTED', ts.ckks_vector(context, col('WAREHOUSE_ID')))
# df.printSchema()
# context = ts.context_from(secret_context)

# select WAREHOUSE_ID, count(WAREHOUSE_ID) from INVENTORIES group by WAREHOUSE_ID order by WAREHOUSE_ID asc;
query = 'select PRODUCT_ID, WAREHOUSE_ID, count(WAREHOUSE_ID) from INVENTORIES group by WAREHOUSE_ID order by WAREHOUSE_ID asc'
dfTempView = df.createTempView('tempInventories')
spark.sql(query).show(10)
# Inventories = Row("PRODUCT_ID","QUANTITY","WAREHOUSE_ID_ENCRYPTED")
# afterList = []
for row in df.collect():
    product_id = row.__getitem__('PRODUCT_ID')
    quantity = row.__getitem__('QUANTITY')
    warehouse_id = row.__getitem__('WAREHOUSE_ID')
    enc_warehouse_id = ts.ckks_vector(context, [warehouse_id]).data
    print('product_id:{} quantity:{} warehouse_id:{} enc_warehouse_id:{}'
          .format(product_id, quantity, warehouse_id, enc_warehouse_id))
    # item=Inventories(product_id,quantity,enc_data)
    # afterList.append(item)

# print(afterList)
