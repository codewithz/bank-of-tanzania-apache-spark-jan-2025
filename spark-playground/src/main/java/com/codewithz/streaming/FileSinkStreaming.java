package com.codewithz.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

public class FileSinkStreaming {

    public static void main(String[] args) {

        // Create Spark Session
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("File Sink")
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

//     Select the columns and rename them

        Dataset<Row> projectedDF=fileStreamDF
                .select(
                        col("Date").alias("date"),
                        col("Country_Code").alias("countryCode"),
                        col("Sold_Units").alias("soldUnits")
                );
        String outputPath="J:\\Zartab\\CodeWithZAcademy\\Spark\\spark-stream-datasets\\historicalDataset\\sinkLocation";
        String checkpointPath="J:\\Zartab\\CodeWithZAcademy\\Spark\\spark-stream-datasets\\historicalDataset\\sinkLocation\\checkpoint";
//        Write the output to JSON Sink
        try{
            projectedDF.writeStream()
                    .outputMode(OutputMode.Append())
                    .format("json")
                    .option("path",outputPath)
                    .option("checkpointLocation",checkpointPath)
                    .start()
                    .awaitTermination();
        }
        catch(Exception e){
            e.printStackTrace();
        }

        }


    }

