package com.codewithz.dataframes;

import com.codewithz.model.Employee;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple3;
import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class CreateDataFrames {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder()
                .appName("DataFrame/Dataset Creation App")
                .master("local[4]")
                .getOrCreate();

        JavaSparkContext context= JavaSparkContext.fromSparkContext(spark.sparkContext());

//        System.out.println("--------------- Way 1 -----------------------------------");
////        Data
//        List<Tuple3<Integer,String,Integer>> data= Arrays.asList(
//                new Tuple3<>(1, "Neha", 10000),
//                new Tuple3<>(2, "Steve", 20000),
//                new Tuple3<>(3, "Kari", 30000),
//                new Tuple3<>(4, "Ivan", 40000),
//                new Tuple3<>(5, "Mohit", 50000)
//        );
//
////        Create a RDD
//
//        JavaRDD<Tuple3<Integer,String,Integer>> employeesRDD=context.parallelize(data);
//
////        Convert the RDD to Dataframe
//
//        Dataset<Tuple3<Integer,String,Integer>> employeeDF = spark.
//                createDataset(employeesRDD.rdd()
//                        , Encoders.tuple(Encoders.INT(),Encoders.STRING(),Encoders.INT()));
//
//        employeeDF.show();



        System.out.println("------------------------Way 2-------------------------------");

        List<Employee> employeeList=Arrays.asList(
                new Employee(1,"Alex",10000),
                new Employee(2,"Mike",20000),
                new Employee(3,"John",30000),
                new Employee(4,"Rick",40000),
                new Employee(5,"Eli",50000)
        );

        JavaRDD<Employee> employeeRDD=context.parallelize(employeeList);

//        Convert this RDD to DataFrame

        Dataset<Employee> employeeDataset=spark.createDataset(
                employeeRDD.rdd(),
                Encoders.bean(Employee.class)
        );

        employeeDataset.show();


        employeeDataset.printSchema();
        System.out.println("----------------------------------------------");
        employeeDataset.show();



        System.out.println("--------------- Way 3 -----------------------------------");
//        Load from Data file -- csv
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
//                                        .option("inferSchema","true")
                .schema(yellowTaxiSchema)
                .option("mode","PERMISSIVE") //DROPMALFORMED |FAILFAST |PERMISSIVE
                .csv(filePath);


        yellowTaxiDF.printSchema();
        yellowTaxiDF.show(false);



        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }

    }
}
