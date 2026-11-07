from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType
)

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("EcommerceStreaming") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Define incoming transaction schema
schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("price", DoubleType()),
    StructField("payment_method", StringType()),
    StructField("timestamp", StringType())
])

# 3. Read streaming data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce_transactions") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Extract JSON messages
json_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_data")

# 5. Convert JSON into structured columns
transactions_df = json_df.select(
    from_json(col("json_data"), schema).alias("data")
).select("data.*")

# 6. Convert timestamp
transactions_df = transactions_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)

# 7. Calculate transaction amount
transactions_df = transactions_df.withColumn(
    "transaction_amount",
    col("quantity") * col("price")
)

# 8. Display transactions
query = transactions_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "./checkpoints/ecommerce") \
    .start()

query.awaitTermination()