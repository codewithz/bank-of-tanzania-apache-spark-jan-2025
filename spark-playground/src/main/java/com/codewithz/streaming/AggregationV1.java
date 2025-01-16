package com.codewithz.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class AggregationV1 {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        // Create Spark Session
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("Aggregations")
                .getOrCreate();

        sparkSession.sparkContext().setLogLevel("ERROR");

        // Define schema
        StructType schema = new StructType()
                .add("car", DataTypes.StringType, true)
                .add("price", DataTypes.DoubleType, true)
                .add("body", DataTypes.StringType, true)
                .add("mileage", DataTypes.DoubleType, true)
                .add("engV", DataTypes.StringType, true)
                .add("engType", DataTypes.StringType, true)
                .add("registration", DataTypes.StringType, true)
                .add("year", DataTypes.IntegerType, true)
                .add("model", DataTypes.StringType, true)
                .add("drive", DataTypes.StringType, true);
        String filePath = "J:\\Zartab\\CodeWithZAcademy\\Spark\\spark-stream-datasets\\carAdsDataset\\dropLocation";

        // Read streaming data
        Dataset<Row> fileStreamDf = sparkSession.readStream()
                .option("header", "true")  // CSV file has a header
                .option("maxFilesPerTrigger", 10)  // Process up to 10 files per trigger
                .schema(schema)
                .csv(filePath);  // Source directory

        // Print schema for debugging
        fileStreamDf.printSchema();

        // Perform groupBy aggregation
        Dataset<Row> aggregationDf = fileStreamDf.groupBy("car")
                .count()
                .withColumnRenamed("count", "car_count");

        // Write output to console
        StreamingQuery query = aggregationDf.writeStream()
                .outputMode("complete")  // Complete mode for full aggregation results
                .format("console")  // Output to console
                .option("numRows", 100)  // Limit to 10 rows per batch
                .start();

        // Await termination
        query.awaitTermination();
    }
}
