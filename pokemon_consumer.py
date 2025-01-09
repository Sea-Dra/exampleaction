from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType

BOOTSTRAP_SERVERS = "confluent-local-broker-1:53705"
TOPIC = "pokemon"

def main():
    spark = SparkSession.builder \
                        .appName('PokemonStats') \
                        .getOrCreate()
    # Read from the datastream
    kafka_stream_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS).option("subscribe",TOPIC).load()
    # Define a table schema for the resultant JSON
    schema = StructType([
        StructField("name", StringType()),
        StructField("id", IntegerType()),
        StructField("color", StringType()),
        StructField("weight", IntegerType()),
        StructField("height", IntegerType()),
        StructField("moves", IntegerType()),
        StructField("is_mythical", BooleanType()),
        StructField("stats", StructType([
            StructField("attack", IntegerType()),
            StructField("defense", IntegerType())
        ]))
    ])
    # Drill down into just the values column
    kafka_stream_df = kafka_stream_df.select(F.from_json(F.col("value").cast("string"),schema).alias("data")).select("data.name","data.id","data.color","data.weight","data.height","data.moves")  
    # Write Dataframe to the console
    kafka_stream_df.writeStream.outputMode("append").format("console").start()

    spark.streams.awaitAnyTermination()
    
    
if __name__ == "__main__":
    main()