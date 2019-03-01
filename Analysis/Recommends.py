from pyspark.sql import SparkSession

# initialise sparkContext
spark = SparkSession.builder \
    .master('local') \
    .appName('YtNetworks') \
    .getOrCreate()

sc = spark.sparkContext

# using SQLContext to read parquet file
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

# to read parquet file
df = sqlContext.read.parquet("C:\\Users\\mark\\Downloads\\Videos.*.parquet")
p = df.limit(1000).toPandas()


print("end")