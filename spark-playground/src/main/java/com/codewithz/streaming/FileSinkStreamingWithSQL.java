package com.codewithz.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

public class FileSinkStreamingWithSQL {

    public static void main(String[] args) {

        // Create Spark Session
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("File with SQL Sink")
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

 // Register the streaming DataFrame as a temporary SQL table
        fileStreamDF.createOrReplaceTempView("streaming_data");

        // Define a SQL query to process the data
        String sqlQuery = "SELECT " +
                "Date AS date, " +
                "Country_Code AS countryCode, " +
                "Sold_Units AS totalUnits " +
                "FROM streaming_data ";


        // Execute the SQL query
        Dataset<Row> sqlResultDF = sparkSession.sql(sqlQuery);
        String outputPath="J:\\Zartab\\CodeWithZAcademy\\Spark\\spark-stream-datasets\\historicalDataset\\sinkLocation";
        String checkpointPath="J:\\Zartab\\CodeWithZAcademy\\Spark\\spark-stream-datasets\\historicalDataset\\sinkLocation\\checkpoint";
//        Write the output to JSON Sink
        try{
            sqlResultDF.writeStream()
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

