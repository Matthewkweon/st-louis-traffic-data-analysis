import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import to_timestamp, hour, dayofweek, month, mean, stddev

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)

# Read the source data
datasource = glueContext.create_dynamic_frame.from_catalog(database="st-louis-traffic", table_name="st-louis-traffic-table")
df = datasource.toDF()

# Handle missing values
df = df.na.fill(0)  # Replace nulls with 0, adjust as needed

# Convert date/time fields
df = df.withColumn("timestamp", to_timestamp("Month2", "MMMM"))

# Create time-based features
df = df.withColumn("hour", hour("timestamp"))
df = df.withColumn("day_of_week", dayofweek("timestamp"))
df = df.withColumn("month", month("timestamp"))

# Normalize numerical features (example with PkHrVol)
from pyspark.sql.functions import mean, stddev
mean_val = df.select(mean("PkHrVol")).collect()[0][0]
stddev_val = df.select(stddev("PkHrVol")).collect()[0][0]
df = df.withColumn("PkHrVol_normalized", (df["PkHrVol"] - mean_val) / stddev_val)

# Write the processed data back to S3
datasink = glueContext.write_dynamic_frame.from_options(
    frame = DynamicFrame.fromDF(df, glueContext, "st-louis-traffic"),
    connection_type = "s3",
    connection_options = {"path": "s3://st-louis-traffic-data/"},
    format = "parquet"
)

job.commit()