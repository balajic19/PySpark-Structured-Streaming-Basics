# Spark UI - localhost:4040
import findspark
findspark.init('C://Spark')

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import os


# spark = SparkSession.builder.master('local[*]').appName('File Streaming Parquet').getOrCreate()

# To setup schema inference
spark = SparkSession.builder.master('local[*]').appName('File Streaming Parquet').config('spark.sql.streaming.schemaInference', 'True').getOrCreate()

data_path = os.path.join(os.getcwd(), 'FileStreaming\input_data\parquet')
print(data_path)


stream_df = spark.readStream.format('parquet').option('header', True)\
    .load(path=data_path)
    # .load(path="C://Users/Balaji/workspace/Python_workspace/PySpark/Intro to Spark Streaming/FileStreaming/input_data/parquet")

spark.sparkContext.setLogLevel('ERROR')

input_parquet_schema = StructType([
    StructField("registration_dttm", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("cc", StringType(), True),
    StructField("country", StringType(), True),
    StructField("birthdate", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("title", StringType(), True),
    StructField("comments", StringType(), True)
])



# stream_df = spark.readStream.format('parquet').schema(input_parquet_schema).option('header', True)\
#     .load(path="C://Users/Balaji/workspace/Python_workspace/PySpark/Intro to Spark Streaming/FileStreaming/input_data/parquet")

write_df = stream_df.writeStream.format('console').start()
stream_df.printSchema()

df1 = stream_df.groupBy('country').count().orderBy('count', ascending=False)

write_df1 = df1.writeStream.format('console').outputMode('complete') \
    .trigger(processingTime='10 seconds').start()
# .option('checkpointLocation', "./FileStreaming/streaming-checkpoint-loc-parquet")\


write_df1.awaitTermination()
write_df.awaitTermination()