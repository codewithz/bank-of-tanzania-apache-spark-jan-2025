package com.codewithz.rdd;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class JobExecutionExample {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder()
                .appName("Job Execution Example")
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
////        List<Tuple2<Integer,Double>> resultsWithPairRdd=sortedByKeyRdd.collect();
////        System.out.println("Numbers with Square Root");
//        for(Tuple2<Integer,Double> result:resultsWithPairRdd){
////            System.out.println(result);
////            System.out.println(result._1);
////            System.out.println(result._2);
////        }




        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }
}
