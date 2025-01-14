package com.codewithz.rdd;

import com.codewithz.model.Employee;
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

        System.out.println("-------------------------------------------------------");

        List<Employee> employeeList=Arrays.asList(
                new Employee(1,"Alex",10000),
                new Employee(2,"Mike",20000),
                new Employee(3,"John",30000),
                new Employee(4,"Rick",40000),
                new Employee(5,"Eli",50000)
        );

        JavaRDD<Employee> employeeRDD=context.parallelize(employeeList);

        List<Employee> employees=employeeRDD.collect();
        System.out.println("Employees:"+employees);

        


        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }
}
