from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col, window, avg

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "crypto_prices"
CASSANDRA_KEYSPACE = "crypto_keyspace"
CASSANDRA_TABLE = "prices"

schema = (
    StructType()
    .add("symbol", StringType())
    .add("price", DoubleType())
    .add("fetched_at", TimestampType())
)

def main():
    spark = (
        SparkSession.builder
        .appName("CryptoStreamProcessor")
        .config("spark.cassandra.connection.host", "cassandra")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = raw_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    agg_df = (
        parsed_df
        .withWatermark("fetched_at", "2 minutes")
        .groupBy(
            window(col("fetched_at"), "1 minute"),
            col("symbol")
        )
        .agg(avg("price").alias("avg_price"))
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("avg_price"),
        )
    )

    query = (
        parsed_df.writeStream
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", CASSANDRA_KEYSPACE)
        .option("table", CASSANDRA_TABLE)
        .option("checkpointLocation", "/tmp/checkpoints/prices")
        .outputMode("append")
        .start()
    )
    def write_agg_batch(batch_df, batch_id):
        (
            batch_df.write
            .format("org.apache.spark.sql.cassandra")
            .option("keyspace", CASSANDRA_KEYSPACE)
            .option("table", "prices_avg_1min")
            .mode("append")
            .save()
        )

    agg_query = (
        agg_df.writeStream
        .foreachBatch(write_agg_batch)
        .option("checkpointLocation", "/tmp/checkpoints/agg")
        .outputMode("update")
        .start()
    )

    query.awaitTermination()
    agg_query.awaitTermination()


if __name__ == "__main__":
    main()