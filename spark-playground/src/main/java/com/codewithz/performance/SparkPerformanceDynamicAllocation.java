package com.codewithz.performance;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Scanner;

import static org.apache.spark.sql.functions.sum;

public class SparkPerformanceDynamicAllocation {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ResourceAllocationApp")
                .master("spark://localhost:7077")
//                .config("spark.dynamicAllocation.enabled", "false")

//                // Define configuration for application
//                .config("spark.cores.max", "4")  // Maximum cores across the cluster
//                .config("spark.executor.memory", "2g")  // Executor memory size
//                .config("spark.executor.cores", "2")  // Number of cores per executor

//                # Enable dynamic allocation
                .config("spark.dynamicAllocation.enabled",                 "true")
                .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")


//                # Define configuration for application
                .config("spark.executor.memory",    "2g")
                .config("spark.executor.cores",     "2")

                .config("spark.executor.instances", "2")

//                # Define minimum and maximum executors
                .config("spark.dynamicAllocation.minExecutors", "0")
                .config("spark.dynamicAllocation.maxExecutors", "5")

//                # Set scheduler backlog timeout
                .config("spark.dynamicAllocation.schedulerBacklogTimeout",   "1s")

//                # Set executor idle timeout
                .config("spark.dynamicAllocation.executorIdleTimeout",       "10s")

//                # Set cached idle timeout
                .config("spark.dynamicAllocation.cachedExecutorIdleTimeout", "10s")


                .getOrCreate();


        System.out.println("------------ For a larger Dataset ------------------");

        //       Load from Data file -- csv
        String filePath="J:\\Zartab\\CodeWithZAcademy\\Spark\\new-data\\YellowTaxis_202210.csv";

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

        // Print the schema for verification
        System.out.println(yellowTaxiSchema.prettyJson());


        Dataset<Row> yellowTaxiDF=spark
                .read()
                .option("header","true")
                .schema(yellowTaxiSchema)
                .csv(filePath);

        Dataset<Row> yellowTaxiGroupedDF=yellowTaxiDF
                .dropDuplicates()
                .groupBy("PULocationID")
                .agg(sum("total_amount").alias("Total Amount"));

        yellowTaxiGroupedDF.show();






        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }
}
