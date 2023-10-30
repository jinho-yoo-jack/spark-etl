from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import expr, col
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
    .appName('hdfs2save') \
    .getOrCreate()

data = [("James ", "", "Smith", "36636", "M", 3000),
        ("Michael ", "Rose", "", "40288", "M", 4000),
        ("Robert ", "", "Williams", "42114", "M", 4000),
        ("Maria ", "Anne", "Jones", "39192", "F", 4000),
        ("Jen", "Mary", "Brown", "", "F", -1)]
columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
df = spark.createDataFrame(data, columns)
