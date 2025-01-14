package com.codewithz.rdd;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;


import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import scala.Tuple2;

public class PairRDDs {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder()
                .appName("Pair RDD Ops")
                .master("local[4]")
                .getOrCreate();


//        SparkConf configuration=new SparkConf().setAppName("RDD Creation App").setMaster("local[4]");

        JavaSparkContext context= JavaSparkContext.fromSparkContext(spark.sparkContext());
        List<Integer> list= Arrays.asList(2,4,5,3,1);

        JavaRDD<Integer> numbersRdd=context.parallelize(list);


        JavaPairRDD<Integer,Double> numsWithSqrtRdd=numbersRdd.mapToPair(
                number -> new Tuple2<>(number,Math.sqrt(number))
        );
        JavaPairRDD<Integer,Double> sortedByKeyRdd=numsWithSqrtRdd.sortByKey();
        List<Tuple2<Integer,Double>> resultsWithPairRdd=sortedByKeyRdd.collect();
        System.out.println("Numbers with Square Root");
        for(Tuple2<Integer,Double> result:resultsWithPairRdd){
            System.out.println(result);
            System.out.println(result._1);
            System.out.println(result._2);
        }


//        JavaRDD<NumberWithSquareRoot> numberWithSquareRootJavaRDD=numbersRdd.map(
//                number -> new NumberWithSquareRoot(number,Math.sqrt(number))
//        );
//
//        List<NumberWithSquareRoot> results=numberWithSquareRootJavaRDD.collect();
//        System.out.println("Numbers with Square Root");
//        for(NumberWithSquareRoot result:results){
//            System.out.println(result);
//        }

        System.out.println("------------------------------------------------------------------");

        String filePath="J:\\Zartab\\CodeWithZAcademy\\Spark\\new-data\\TaxiZones.csv";

        JavaRDD<String> taxiZonesRdd=context.textFile(filePath);

        JavaRDD<String[]> taxiZonesWithColsRdd=taxiZonesRdd.map(
                line -> line.split(",")
        );

//        Use taxiZonesWithColsRdd and create a Pair RDD (LocationId as Key, Zone Details as Value)
        //[['1', 'EWR', 'Newark Airport', 'EWR'],
        // ['2', 'Queens', 'Jamaica Bay', 'Boro Zone'],
        // ['3', 'Bronx', 'Allerton/Pelham Gardens', 'Boro Zone'],
        // ['4', 'Manhattan', 'Alphabet City', 'Yellow Zone'],
        // ['5', 'Staten Island', 'Arden Heights', 'Boro Zone']]

        JavaPairRDD<String,String[]> taxiZonesPairRdd=taxiZonesWithColsRdd.mapToPair(
                zoneRow -> new Tuple2<>(zoneRow[0],zoneRow)
        );

        List<Tuple2<String,String[]>> first10Zones=taxiZonesPairRdd.take(10);
        for(Tuple2<String,String[]> result:first10Zones){
            System.out.println(result._1 +" - "+Arrays.toString(result._2));

        }

        System.out.println("-------------# Calculate count of records for each Borough---------");
//        ['1', 'EWR', 'Newark Airport', 'EWR'],
        // ['2', 'Queens', 'Jamaica Bay', 'Boro Zone'],
        // ['3', 'Bronx', 'Allerton/Pelham Gardens', 'Boro Zone'],
        // ['4', 'Manhattan', 'Alphabet City', 'Yellow Zone'],
        // ['5', 'Staten Island', 'Arden Heights', 'Boro Zone']

        JavaPairRDD<String,Integer> taxiZonesBoroughPairRdd=taxiZonesWithColsRdd.mapToPair(
                zoneRow -> new Tuple2<>(zoneRow[1],1)
        );

        List<Tuple2<String,Integer>> intermediateResult=taxiZonesBoroughPairRdd.collect();
        for(Tuple2<String,Integer> result:intermediateResult){
            System.out.println(result._1 +" - "+result._2);

        }

        System.out.println("-------------------------------------------------------");
        JavaPairRDD<String,Integer> boroughCountRdd=taxiZonesBoroughPairRdd.reduceByKey(
                (accumulator, currentValue) -> accumulator + currentValue
        );

        List<Tuple2<String,Integer>> boruoghCountResults=boroughCountRdd.collect();
        for(Tuple2<String,Integer> result:boruoghCountResults){
            System.out.println(result._1 +" - "+result._2);

        }

        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }
}
