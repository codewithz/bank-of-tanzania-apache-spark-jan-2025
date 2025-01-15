package com.codewithz.dataframes;

import org.apache.spark.sql.SparkSession;
import java.util.*;
import org.apache.spark.sql.*;

//We will load data from Postgres Table where the transaction value is in USD
//THis spark code will load the data from postgres,get it into a dataframe,
//write a transformation to convert the transaction value in Tanzanian Shillings
//and write the new dataframe to mysql table
public class PGToMySQLPipeline {

    public static void main(String[] args) {
        // Initialize Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("Postgres to MySQL Transformation")
                .master("local[*]")
                .getOrCreate();

        // PostgreSQL Configuration
        String postgresUrl = "jdbc:postgresql://localhost:5432/bank_of_tanzania";
        String postgresTable = "usd_transactions";
        String postgresUser = "postgres";
        String postgresPassword = "admin";

        // MySQL Configuration
        String mysqlUrl = "jdbc:mysql://localhost:3306/bank_of_tanzania";
        String mysqlTable = "transformed_transactions";
        String mysqlUser = "root";
        String mysqlPassword = "admin";

        // Set up JDBC properties
        Properties postgresProps = new Properties();
        postgresProps.put("user", postgresUser);
        postgresProps.put("password", postgresPassword);
        postgresProps.put("driver", "org.postgresql.Driver");

        Properties mysqlProps = new Properties();
        mysqlProps.put("user", mysqlUser);
        mysqlProps.put("password", mysqlPassword);
        mysqlProps.put("driver", "com.mysql.cj.jdbc.Driver");

//        Load the data from PG into DF

        Dataset<Row> transactionsDF = spark.read()
                .jdbc(postgresUrl, postgresTable, postgresProps);

//        Transformation
//        Dataset<Row> transaformedDF=transactionsDF
//                .withColumn("amount_tzs",transactionsDF.col("amount_usd").multiply(2300))
//                .select(
//                        functions.col("tx_id"),
//                        functions.col("bank_name"),
//                        functions.col("account_holder"),
//                        functions.col("amount_tzs"),
//                        functions.col("timestamp")
//
//                );


        // Register DataFrame as a temporary SQL view
        transactionsDF.createOrReplaceTempView("transactions_view");

        // Execute SQL query for transformation
        Dataset<Row> transformedDF = spark.sql(
                "SELECT " +
                        "tx_id, " +
                        "bank_name, " +
                        "account_holder, " +
                        "(amount_usd * 2300) AS amount_tzs, " +
                        "timestamp " +
                        "FROM transactions_view"
        );


        transformedDF
                .write()
                .mode("overwrite")
                .jdbc(mysqlUrl,mysqlTable,mysqlProps);


        System.out.println("Data successfully transformed and written to MySQL table!");


        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }



    }
}
