package com.codewithz.rdd;

import com.codewithz.model.Employee;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class CreatingRDDFromFile {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("RDD Creation App from File")
                .master("local[4]")
                .getOrCreate();

        JavaSparkContext context= JavaSparkContext.fromSparkContext(spark.sparkContext());

        String fileLocation="J:\\Zartab\\CodeWithZAcademy\\Spark\\new-data\\TaxiZones.csv";

        JavaRDD<String> taxiZonesRDD = context.textFile(fileLocation);

//        taxiZonesRDD.take(5).forEach(System.out::println);


        JavaRDD<String[]> taxiZonesWithColsRDD=taxiZonesRDD.map(line->line.split(","));
        //        Filter rows where the index 1 is Manhattan and index 2 starts with Central

        JavaRDD<String[]> filteredZoneRDD=taxiZonesWithColsRDD
                .filter(
                        zoneRow->zoneRow[1].equals("Manhattan") &&
                                zoneRow[2].toLowerCase().startsWith("central")
                );

        List<String[]> filteredTaxiZoneOutput=filteredZoneRDD.collect();

        for(String[] zone:filteredTaxiZoneOutput){
            System.out.println(Arrays.toString(zone));
        }



        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }
}
