package com.codewithz.kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class TransactionStreamProcessor {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        // Spark session initialization
        SparkSession spark = SparkSession.builder()
                .appName("TransactionStreamProcessor")
                .master("local[4]")
                .getOrCreate();


        // Define the schema of the Transaction data
        StructType transactionSchema = new StructType()
                .add("txId", DataTypes.StringType)
                .add("bankName", DataTypes.StringType)
                .add("accountHolder", DataTypes.StringType)
                .add("amount", DataTypes.DoubleType)
                .add("balanceAmount", DataTypes.DoubleType)
                .add("country", DataTypes.StringType)
                .add("state", DataTypes.StringType)
                .add("timestamp", DataTypes.LongType)
                .add("txTypeCode", DataTypes.IntegerType);


        Dataset<Row> kafkaStream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("kafka.group.id", "spark-app-cg")
                .option("subscribe", "bot_raw_tx")
                .option("startingOffsets", "latest")
                .load();

        // Deserialize Kafka value and apply schema
        Dataset<Row> transactionStream = kafkaStream
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), transactionSchema).as("transaction"))
                .select("transaction.*");


        // Filter and transform data
        Dataset<Row> validTransactions = transactionStream
                .filter(col("amount").gt(100))
                .withColumn("txType", when(col("txTypeCode").equalTo(1), lit("Savings"))
                        .when(col("txTypeCode").equalTo(2), lit("Credit"))
                        .when(col("txTypeCode").equalTo(3), lit("Checkings"))
                        .otherwise(lit("Unknown")));

        // Serialize the transformed data back to JSON for Kafka output
        Dataset<Row> kafkaOutput = validTransactions
                .selectExpr("to_json(struct(*)) AS value");

        // Write the transformed data to another Kafka topic
        kafkaOutput.writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", "bot_spark_valid_tx")
                .option("checkpointLocation", "J:\\Zartab\\CodeWithZAcademy\\Spark\\spark-stream-datasets\\checkpoint")
                .outputMode("append")
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .start()
                .awaitTermination();
    }


}
