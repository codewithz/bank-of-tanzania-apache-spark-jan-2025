package com.codewithz.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;

public class ForEachSinkStreaming {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        // Create Spark Session
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("FOr Each Sink")
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

        Dataset<Row> countDF=fileStreamDF.groupBy("Country_Code").count();

        Dataset<Row> processedDF=countDF.select("Country_Code","count");

        StreamingQuery query=processedDF.writeStream()
                .foreach(new ForeachWriter<Row>() {
                    @Override
                    public boolean open(long partitionId, long epochId) {
                        return true; //Open connection or resource
                    }

                    @Override
                    public void process(Row row) {
                    // Process each row
                        System.out.println("------------");
                        System.out.println("Country_Code: " + row.getAs("Country_Code"));
                        System.out.println("Count: " + row.getAs("count"));
                    }

                    @Override
                    public void close(Throwable errorOrNull) {
                    //close the connection
                    }
                })
                .outputMode(OutputMode.Update())
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .start();


        query.awaitTermination();

        }


    }

