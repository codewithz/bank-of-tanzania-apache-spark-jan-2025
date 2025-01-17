package com.codewithz.performance;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Scanner;

public class SparkPerformanceJoins {

    public static void main(String[] args) {


        SparkSession spark=SparkSession.builder()
                .appName("Spark Performance - Joins ")
                .master("local[4]")
                .getOrCreate();

        StructType taxiZonesSchema = new StructType()
                .add("LocationID", DataTypes.IntegerType, true)
                .add("Borough", DataTypes.StringType, true)
                .add("Zone", DataTypes.StringType, true)
                .add("ServiceZone", DataTypes.StringType, true);

        Dataset<Row> taxiZonesDF=spark
                .read()
                .option("header","true")
                .option("inferSchema","true")
                .schema(taxiZonesSchema)
                .csv("J:\\Zartab\\CodeWithZAcademy\\Spark\\new-data\\TaxiZones.csv");



        System.out.println("Taxi DF Counts : "+taxiZonesDF.count());

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

        System.out.println("Yellow TAxi DF Count:"+yellowTaxiDF.count());

        String broadcastThreshold=spark.conf().get("spark.sql.autoBroadcastJoinThreshold");

        System.out.println("Broadcast Join Threshold is "+broadcastThreshold);

//        spark.conf().set("spark.sql.autoBroadcastJoinThreshold","-1"); // Disabling broadcast join

        Dataset<Row> joinedDF=yellowTaxiDF.join(
                taxiZonesDF,
                yellowTaxiDF.col("PULocationId").equalTo(taxiZonesDF.col("LocationID")),
                "inner"
        );

        joinedDF.show();

        yellowTaxiDF.createOrReplaceTempView("YellowTaxi1");
        yellowTaxiDF.createOrReplaceTempView("YellowTaxi2");

        spark.sql("" +
                "Select * from " +
                "YellowTaxi1 yt1 " +
                "JOIN YellowTaxi2 yt2 " +
                "ON yt1.PULocationID = yt2.DOLocationID").show();


        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }
}