
import os
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("RustFSConnectionTest") \
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.5.0"
    ) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", os.environ["RUSTFS_ACCESS_KEY"]) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["RUSTFS_SECRET_KEY"]) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Create sample data
data = [
    ("TXN001", "C001", 100.50),
    ("TXN002", "C002", 250.75),
    ("TXN003", "C003", 75.25)
]

columns = ["transaction_id", "customer_id", "amount"]

df = spark.createDataFrame(data, columns)

# Write data to RustFS
output_path = "s3a://ecommerce-bronze/connectivity-test/"

df.write.mode("overwrite").parquet(output_path)

print("SUCCESS: Data written to RustFS!")

spark.stop()

