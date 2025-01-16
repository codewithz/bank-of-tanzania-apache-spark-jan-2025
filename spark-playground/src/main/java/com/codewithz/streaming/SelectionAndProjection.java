package com.codewithz.streaming;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.*;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class SelectionAndProjection {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        // Create SparkSession
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("Selections and Projections")
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
                .option("header", "true")
                .option("maxFilesPerTrigger", 1) // Process one file per trigger
                .schema(schema)
                .csv(filePath);

        // Print schema for debugging
        fileStreamDf.printSchema();

        // Define the age category column
        Column ageCategoryColumn = expr("IF(price > 70000, 'premium', 'regular')");

        // Apply projections and filters
        Dataset<Row> projectionsDf = fileStreamDf
                .select(
                        fileStreamDf.col("car"),
                        fileStreamDf.col("model"),
                        fileStreamDf.col("price"),
                        fileStreamDf.col("mileage"),
                        fileStreamDf.col("year")
                )
                .where(fileStreamDf.col("year").gt(2014)) // Filter rows with year > 2014
                .filter("price != 0") // Exclude rows where price is 0
                .withColumn("age_category", ageCategoryColumn); // Add age category column

        // Write the stream output to console
        StreamingQuery query = projectionsDf.writeStream()
                .outputMode("append") // Append mode for streaming data
                .format("console") // Output to console
                .option("numRows", 10) // Display 10 rows per batch
                .start();

        // Wait for termination
        query.awaitTermination();
    }
}

