package com.codewithz.hdfs;



import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.sum;

public class HDFSConnect {

    public static void main(String[] args) {
        // Create SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("HDFS Read Write Example")
                .master("spark://127.100.100.1:7077") // Spark master address
                .config("spark.hadoop.fs.defaultFS", "hdfs://127.100.100.1:8020") // HDFS configuration
                .getOrCreate();

        System.out.println("------------ Reading Dataset from HDFS ------------------");

        // Define schema for the dataset
        StructType yellowTaxiSchema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("VendorId", DataTypes.IntegerType, true),
                DataTypes.createStructField("lpep_pickup_datetime", DataTypes.TimestampType, true),
                DataTypes.createStructField("lpep_dropoff_datetime", DataTypes.TimestampType, true),
                DataTypes.createStructField("passenger_count", DataTypes.DoubleType, true),
                DataTypes.createStructField("trip_distance", DataTypes.DoubleType, true),
                DataTypes.createStructField("RatecodeID", DataTypes.DoubleType, true),
                DataTypes.createStructField("store_and_fwd_flag", DataTypes.StringType, true),
                DataTypes.createStructField("PULocationID", DataTypes.IntegerType, true),
                DataTypes.createStructField("DOLocationID", DataTypes.IntegerType, true),
                DataTypes.createStructField("payment_type", DataTypes.IntegerType, true),
                DataTypes.createStructField("fare_amount", DataTypes.DoubleType, true),
                DataTypes.createStructField("extra", DataTypes.DoubleType, true),
                DataTypes.createStructField("mta_tax", DataTypes.DoubleType, true),
                DataTypes.createStructField("tip_amount", DataTypes.DoubleType, true),
                DataTypes.createStructField("tolls_amount", DataTypes.DoubleType, true),
                DataTypes.createStructField("improvement_surcharge", DataTypes.DoubleType, true),
                DataTypes.createStructField("total_amount", DataTypes.DoubleType, true),
                DataTypes.createStructField("congestion_surcharge", DataTypes.DoubleType, true),
                DataTypes.createStructField("airport_fee", DataTypes.DoubleType, true)
        });

        // Read the dataset from HDFS
        String hdfsInputPath = "hdfs://127.100.100.1:8020/user/spark/yellow_taxi_data.csv";

        Dataset<Row> yellowTaxiDF = spark.read()
                .option("header", "true")
                .schema(yellowTaxiSchema)
                .csv(hdfsInputPath);

        // Perform transformations: Drop duplicates and aggregate data
        Dataset<Row> yellowTaxiGroupedDF = yellowTaxiDF
                .dropDuplicates()
                .groupBy("PULocationID")
                .agg(sum("total_amount").alias("Total Amount"));

        // Show the aggregated result
        yellowTaxiGroupedDF.show();

        System.out.println("------------ Writing Results to HDFS ------------------");

        // Write the transformed dataset back to HDFS
        String hdfsOutputPath = "hdfs://127.100.100.1:8020/user/spark/output/yellow_taxi_aggregated";

        yellowTaxiGroupedDF.write()
                .mode("overwrite") // Overwrite if the folder exists
                .csv(hdfsOutputPath);

        System.out.println("Data successfully written to HDFS.");
    }
}

