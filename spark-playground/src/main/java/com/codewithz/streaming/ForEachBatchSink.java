package com.codewithz.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class ForEachBatchSink {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("Foreach Batch Sink")
                .getOrCreate();

        sparkSession.sparkContext().setLogLevel("ERROR");

        // Define schema
        StructType schema = new StructType()
                .add("Date", DataTypes.StringType, false)
                .add("Article_ID", DataTypes.StringType, false)
                .add("Country_Code", DataTypes.StringType, false)
                .add("Sold_Units", DataTypes.IntegerType, false);

        String streamPath="J:\\Zartab\\CodeWithZAcademy\\Spark\\spark-stream-datasets\\historicalDataset\\dropLocation";

        // Read the streaming data
        Dataset<Row> fileStreamDf = sparkSession.readStream()
                .option("header", "true")
                .schema(schema)
                .csv(streamPath);

        System.out.println(" ");
        System.out.println("Is the stream ready? " + fileStreamDf.isStreaming());

        System.out.println(" ");
        fileStreamDf.printSchema();

        // Perform aggregation
        Dataset<Row> countDf = fileStreamDf.groupBy("Country_Code").count();

        // Define the foreachBatch logic
        StreamingQuery query = countDf.writeStream()
                .foreachBatch((df, batchId) -> {
                    System.out.println("Batch ID: " + batchId);
                    df.show();

                    String filePath = "J:\\Zartab\\CodeWithZAcademy\\Spark\\spark-stream-datasets\\historicalDataset\\foreachBatch_dir\\" + batchId;

                    // Save the batch data to a CSV file
                    df.repartition(1)
                            .write()
                            .mode(SaveMode.ErrorIfExists)
                            .csv(filePath);

                })
                .outputMode("update") // Use "update" output mode for incremental updates
                .trigger(Trigger.ProcessingTime("5 seconds")) // Trigger every 5 seconds
                .start();

        query.awaitTermination();
    }
}
