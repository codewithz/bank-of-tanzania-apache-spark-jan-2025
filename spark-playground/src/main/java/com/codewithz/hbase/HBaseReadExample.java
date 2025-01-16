package com.codewithz.hbase;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HBaseReadExample {

    public static void main(String[] args) {
        // Create SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("HBase Read Example")
                .master("local[*]") // Use Spark cluster or local mode
                .getOrCreate();

        // HBase Configuration
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "127.100.100.1"); // HBase Zookeeper address
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181"); // Zookeeper client port

        // HBase Table Schema Configuration
        String hbaseTableSchema = "{"
                + "\"table\":{\"namespace\":\"default\", \"name\":\"test_table\"},"
                + "\"rowkey\":\"key\","
                + "\"columns\":{"
                + "\"col1\":{\"cf\":\"cf\", \"col\":\"col1\", \"type\":\"string\"},"
                + "\"col2\":{\"cf\":\"cf\", \"col\":\"col2\", \"type\":\"int\"}"
                + "}"
                + "}";

        // Read data from HBase
        Dataset<Row> hbaseDF = spark.read()
                .option(HBaseTableCatalog.tableCatalog(), hbaseTableSchema)
                .format("org.apache.hadoop.hbase.spark")
                .load();

        // Show the DataFrame content
        hbaseDF.show();

        // Perform additional transformations or actions
        hbaseDF.printSchema();
    }
}
