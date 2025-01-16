package com.codewithz.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ConsoleSinkStreaming {

    public static void main(String[] args) {

        // Create Spark Session
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("Console Sink")
                .getOrCreate();

        sparkSession.sparkContext().setLogLevel("ERROR");

        // Define Schema
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("Date", DataTypes.StringType, false),
                DataTypes.createStructField("Article_ID", DataTypes.StringType, false),
                DataTypes.createStructField("Country_Code", DataTypes.StringType, false),
                DataTypes.createStructField("Sold_Units", DataTypes.IntegerType, false)
        });

//        Read the stream
        String streamPath="J:\\Zartab\\CodeWithZAcademy\\Spark\\spark-stream-datasets\\historicalDataset\\dropLocation";
        Dataset<Row> fileStreamDF=sparkSession.readStream()
                .option("header","true")
                .schema(schema)
                .csv(streamPath);



//      Check if Stream is ready

        System.out.println("\n\n Is the stream ready? "+fileStreamDF.isStreaming());

        System.out.println("---------------------------------------------------");
        System.out.println("\nStream schema:");
        fileStreamDF.printSchema();
        System.out.println("---------------------------------------------------");

        Dataset<Row> selectedDF=fileStreamDF.select("*");

//        Write to Console Sink
        try {
            selectedDF.writeStream()
                    .outputMode(OutputMode.Append())
                    .format("console")
                    .option("numRows", 10)
                    .start()
                    .awaitTermination();
        }catch (Exception e) {
            e.printStackTrace();
        }

        }


    }

