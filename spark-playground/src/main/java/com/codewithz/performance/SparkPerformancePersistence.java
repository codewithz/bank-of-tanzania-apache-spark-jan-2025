package com.codewithz.performance;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.util.Scanner;

import static  org.apache.spark.sql.functions.sum;

public class SparkPerformancePersistence {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Spark Performance - Persistence ")
                .master("local[4]")
                .getOrCreate();

        spark.conf().set("spark.sql.shuffle.partitions", 3);

        JavaSparkContext context = new JavaSparkContext(spark.sparkContext());

        //       Load from Data file -- csv
        String filePath="J:\\Zartab\\CodeWithZAcademy\\Spark\\new-data\\YellowTaxis_202210.csv";

        StructType yellowTaxiSchema = DataTypes.createStructType(new StructField[]{
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

        Dataset<Row> yellowTaxiDF = spark
                .read()
                .option("header", "true")
                .schema(yellowTaxiSchema)
                .csv(filePath);

        Dataset<Row> yellowTaxiGroupedDF = yellowTaxiDF
                .dropDuplicates()
                .groupBy("PULocationID")
                .agg(sum("total_amount").alias("Total Amount"));


//        yellowTaxiGroupedDF.write()
//                .option("header","true")
//                .option("dateFormat","yyyy-MM-dd HH:mm:ss.S")
//                .mode(SaveMode.Overwrite)
//                .csv("J:\\Zartab\\CodeWithZAcademy\\Spark\\outputs\\CacheTest_Enabled_FirstTime.csv");

        //        This is where this will not be persisted -- LAZY OPERATION

        yellowTaxiGroupedDF.persist(StorageLevel.MEMORY_AND_DISK());

               yellowTaxiGroupedDF.write()
                .option("header","true")
                .option("dateFormat","yyyy-MM-dd HH:mm:ss.S")
                .mode(SaveMode.Overwrite)
                .csv("J:\\Zartab\\CodeWithZAcademy\\Spark\\outputs\\CacheTest_Enabled_And_Cached_FirstTime.csv");


//        Write the Cached  DF to CSV for second time  -- this where the caching will happen
        yellowTaxiGroupedDF.write()
                .option("header","true")
                .option("dateFormat","yyyy-MM-dd HH:mm:ss.S")
                .mode(SaveMode.Overwrite)
                .csv("J:\\Zartab\\CodeWithZAcademy\\Spark\\outputs\\CacheTest_Enabled_And_Cached_SecondTime.csv");

        yellowTaxiGroupedDF.unpersist();

        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }
}
