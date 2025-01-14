package com.codewithz.rdd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class CreatingRDDFromCollection {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("RDD Creation App")
                .master("local[4]")
                .getOrCreate();

        JavaSparkContext context=JavaSparkContext.fromSparkContext(spark.sparkContext());

        List<Integer> list= Arrays.asList(1,2,3,4,5,6,7,8,9,10);

        JavaRDD<Integer> rdd=context.parallelize(list);

        int numberOfPartitions=rdd.getNumPartitions();
        System.out.println("Number of Partitions: "+numberOfPartitions);

        List<Integer> output=rdd.collect();

        System.out.println("Output:"+output);


        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }
}
