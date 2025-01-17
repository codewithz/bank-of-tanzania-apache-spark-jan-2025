package com.codewithz.performance;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.*;

import java.util.Scanner;

public class SparkPerformancePartitions {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder()
                .appName("Spark Performance - Partitions ")
                .master("local[4]")
                .getOrCreate();

        JavaSparkContext context=new JavaSparkContext(spark.sparkContext());

        System.out.println("Default Parallelism: "+context.defaultParallelism());
        System.out.println("Default Min Partitions: "+context.defaultMinPartitions());

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

        System.out.println("Partitions :"+taxiZonesDF.rdd().getNumPartitions());

        System.out.println("Record Counts : "+taxiZonesDF.count());

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


        spark.conf().set("spark.sql.files.maxPartitionBytes","64m");

        Dataset<Row> yellowTaxiDF=spark
                .read()
                .option("header","true")
                .schema(yellowTaxiSchema)
                .csv(filePath);

        System.out.println("Partitions :"+yellowTaxiDF.rdd().getNumPartitions());

//        System.out.println("Record Counts : "+yellowTaxiDF.count());
        long count=yellowTaxiDF.count();

//        if(count >2000000){
//            spark.conf().set("something","something");
//        }

        spark.conf().set("spark.sql.shuffle.partitions",3);
        Dataset<Row> yellowTaxiGroupedDF = yellowTaxiDF
                .groupBy("PULocationID")
                .agg(sum("total_amount").alias("TotalAmount"));

//        System.out.println("Partitions for grouped data:"+yellowTaxiGroupedDF.rdd().getNumPartitions());
//
//        System.out.println("Record Counts grouped data : "+yellowTaxiGroupedDF.count());
//        getDataFrameStats(yellowTaxiGroupedDF,"PULocationID").show();
//        getDataFrameStats(yellowTaxiDF,"PULocationID").show();

//        Implementing Repartitioning -- Round Robin
//
//        Dataset<Row> repartitionedDF=yellowTaxiDF.repartition(14);
//
//        System.out.println("Partitions for repartitioned Data:"+repartitionedDF.rdd().getNumPartitions());
//
//        getDataFrameStats(repartitionedDF,"PULocationID").show();
//
//        //        Implementing Repartitioning -- Hash Repartitioning
////
//        Dataset<Row> repartitionedDF=yellowTaxiDF.repartition(14,col("PULocationID"));
//
//        System.out.println("Partitions for repartitioned Data:"+repartitionedDF.rdd().getNumPartitions());
//
//        getDataFrameStats(repartitionedDF,"PULocationID").show();
//
        //        Implementing Repartitioning -- Range Partitioning

        Dataset<Row> repartitionedDF=yellowTaxiDF.repartitionByRange(14,col("PULocationID"));

        repartitionedDF=repartitionedDF.withColumn("PID",spark_partition_id());

        repartitionedDF.createOrReplaceTempView("repartitioned");

        spark.sql("Select * from repartitioned where PID=2").show();

        System.out.println("Partitions for repartitioned Data:"+repartitionedDF.rdd().getNumPartitions());


        getDataFrameStats(repartitionedDF,"PULocationID").show();
//
//
        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }


    }

    // Method to get DataFrame statistics
    public static Dataset<Row> getDataFrameStats(Dataset<Row> dataFrame, String columnName) {
        return dataFrame
                // Get partition number for each record
                .withColumn("Partition Number", functions.spark_partition_id())

                // Group by partition and calculate stats for the specified column
                .groupBy("Partition Number")
                .agg(
                        count("*").alias("Record Count"),
                        min(columnName).alias("Min Column Value"),
                        max(columnName).alias("Max Column Value")
                )
                // Order the results by partition number
                .orderBy("Partition Number");
    }
}
