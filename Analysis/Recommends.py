from pyspark.sql import SparkSession

# initialise sparkContext
spark = SparkSession.builder \
    .master('127.0.0.1:8888') \
    .appName('YtNetworks') \
    .getOrCreate()

sc = spark.sparkContext

# using SQLContext to read parquet file
from pyspark.sql import SQLContext
sql = SQLContext(sc)

# to read parquet file
df = sql.read.text("https://ytnetworks.blob.core.windows.net/data/db/VideoCaptions/UC-3jIAlnQmbbVMV6gR7K8aQ/ThIUvYmDpC8.txt")

df.show