# Spark UI: localhost:4040
import findspark
findspark.init('C:\\Spark')

from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
        .appName('PySpark Application') \
            .master('local[*]') \
                .getOrCreate()

# df_rows = [Row(id=1, name='Balaji', city='Hyderabad'), Row(id=2, name='Pari', city='Chennai')]
# df = spark.createDataFrame(df_rows)
# df.show()

spark.sparkContext.setLogLevel('ERROR')

KAFKA_BOOTSTRAP_CONS = 'localhost:9092'
KAFKA_TOPIC = 'msgtopic1'
df = spark.readStream\
    .format('kafka')\
        .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP_CONS)\
            .option('subscribe', KAFKA_TOPIC)\
                .option('startingOffsets', 'latest') \
                    .load()



print(df.isStreaming)
df.printSchema()

data_df = df.selectExpr("CAST(value AS STRING)", "timestamp")

# Define a Schema for the data

data_schema_string = "transaction_id INT, transaction_card_type STRING, "\
    + "transaction_amount DOUBLE, transaction_datetime STRING"


data_df1 = data_df.select(from_csv(col('value'), data_schema_string).alias('transactions'), "timestamp")

data_df2 = data_df1.select('transactions.*', 'timestamp')
data_df2.printSchema()

data_df3 = data_df2.groupBy('transaction_card_type') \
    .agg({'transaction_amount': 'max'}) \
        .select('transaction_card_type', col('max(transaction_amount)').alias('max transaction')
        )
write_query = data_df3 \
    .writeStream \
        .format('console').outputMode('complete').option('checkpointLocation', './KafkaStreaming/checkpoint-loc-kafka-csv').option('truncate', 'false') \
            .start()

write_query.awaitTermination()
