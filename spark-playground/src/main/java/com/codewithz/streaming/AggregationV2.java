package com.codewithz.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;

public class AggregationV2 {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        // Create SparkSession
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .config("spark.sql.shuffle.partitions", "4") // Reduce shuffle partitions (default is 200)
                .config("spark.executor.memory", "2g") // Increase executor memory
                .config("spark.sql.streaming.stateStore.rocksdb.enable", "true") // Use RocksDB state store (if available)
                .appName("Aggregationsv2")
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
                .option("header", "true")  // CSV files have headers
                .schema(schema)
                .csv(filePath);  // Source directory

        // Print schema for debugging
        fileStreamDf.printSchema();

        // Perform groupBy, aggregation, and ordering
        Dataset<Row> aggregationDf = fileStreamDf.groupBy("year", "car")
                .agg(avg("price").alias("average_price"))  // Calculate average price
                .orderBy(col("year").desc(), col("car").desc());  // Order by year (descending) and car (descending)
        aggregationDf=aggregationDf.repartition(4);
        // Write the stream output to the console
        StreamingQuery query = aggregationDf.writeStream()
                .outputMode("complete")  // Complete mode to output all aggregation results
                .format("console")  // Write results to console
                .option("numRows", 10)  // Limit rows displayed to 10
                .start();

        // Wait for termination
        query.awaitTermination();
    }
}
