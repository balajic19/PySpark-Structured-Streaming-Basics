# Spark UI - localhost:4040
import findspark
findspark.init('C://Spark')

from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField,
 StringType, IntegerType, DoubleType)

import os

spark = SparkSession \
    .builder \
        .master('local[*]') \
            .appName('Streaming from File') \
                .getOrCreate()

# To setup schema inference
# spark = SparkSession.builder.master('local[*]').appName('File Streaming Parquet').config('spark.sql.streaming.schemaInference', 'True').getOrCreate()
# stream_df = spark.readStream.format('parquet').option('header', True)\
#     .load(path="C://Users/Balaji/workspace/Python_workspace/PySpark/Intro to Spark Streaming/FileStreaming/input_data/parquet")
spark.sparkContext.setLogLevel('ERROR')

# registration_dttm,id,first_name,last_name,email,gender,ip_address,cc,country,birthdate,salary,title,comments
input_csv_schema = StructType([
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

data_path = os.path.join(os.getcwd(), 'FileStreaming\input_data\csv')
print(data_path)
stream_df = spark \
    .readStream \
        .format('csv') \
            .schema(input_csv_schema) \
                .option('header', True) \
                    .load(path = data_path)
                # .load(path="C://Users/Balaji/workspace/Python_workspace/PySpark/Intro to Spark Streaming/FileStreaming/input_data/csv")

stream_df.printSchema()



write_df = stream_df \
    .writeStream \
        .format('console').outputMode('update').trigger(processingTime='2 seconds')\
            .start()

# write_df = stream_df \
#     .writeStream \
#         .format('console')\
#             .start()


write_df.awaitTermination()
