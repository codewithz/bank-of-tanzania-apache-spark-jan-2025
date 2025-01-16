package com.codewithz.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class AggregationV3 {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        // Create SparkSession with optimized configurations
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]") // Use all available cores
                .appName("Optimized Aggregations")
                .config("spark.sql.shuffle.partitions", "4") // Reduce shuffle partitions (default is 200)
                .config("spark.executor.memory", "2g") // Increase executor memory
                .config("spark.sql.streaming.stateStore.rocksdb.enable", "true") // Use RocksDB state store (if available)
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
                .option("header", "true") // CSV has headers
                .schema(schema)
                .csv(filePath); // Source directory

        // Print schema for debugging
        fileStreamDf.printSchema();

        // Apply filter to exclude rows with price = 0
        Dataset<Row> filteredDf = fileStreamDf.filter(fileStreamDf.col("price").notEqual(0));

        // Create a temporary view for SQL queries
        filteredDf.createOrReplaceTempView("car_ads");

        // SQL query for aggregation
        Dataset<Row> aggregationDf = sparkSession.sql(
                "SELECT car AS make, " +
                        "AVG(price) AS avg_price, " +
                        "MIN(price) AS min_price, " +
                        "MAX(price) AS max_price " +
                        "FROM car_ads " +
                        "WHERE year > 2013 " +
                        "GROUP BY make"
        );

        // Repartition the output to reduce shuffle overhead
        Dataset<Row> optimizedDf = aggregationDf.repartition(4); // Adjust based on the number of cores

        // Write the stream output to the console
        StreamingQuery query = optimizedDf.writeStream()
                .outputMode("complete") // Complete mode for full aggregation results
                .format("console") // Output results to the console
                .option("numRows", 10) // Display 10 rows per batch
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds")) // Trigger every 10 seconds
                .start();

        // Wait for termination
        query.awaitTermination();
    }
}
