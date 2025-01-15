package com.codewithz.dataframes;

import com.codewithz.model.Transaction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.count;

import java.util.Scanner;

public class CreateDataFrameFromDB {
    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder()
                .appName("DataFrame/Dataset Creation with DB")
                .master("local[4]")
                .getOrCreate();

        // JDBC Configuration
        String jdbcUrl = "jdbc:postgresql://localhost:5432/bank_of_tanzania";
        String dbTable = "transactions"; // Replace with your table name
        String dbUser = "postgres";
        String dbPassword = "admin";

        // Load DataFrame from PostgreSQL
        Dataset<Row> transactionDF = spark.read()
                .format("jdbc")
                .option("url", jdbcUrl)
                .option("dbtable", dbTable)
                .option("user", dbUser)
                .option("password", dbPassword)
                .load();

        transactionDF = transactionDF
                .withColumnRenamed("tx_id", "txId")
                .withColumnRenamed("bank_name", "bankName")
                .withColumnRenamed("account_holder", "accountHolder")
                .withColumnRenamed("balance_amount", "balanceAmount")
                .withColumnRenamed("tx_type_code", "txTypeCode");

        // Convert DataFrame to Dataset<Transaction>
        Dataset<Transaction> transactionDataset = transactionDF.as(Encoders.bean(Transaction.class));

        // Show Dataset
        transactionDataset.show();

//        Dataset<Row> result=transactionDF
//                    .groupBy("bankName")
//                .agg(count("*").alias("TotalTransactions"));
//
//        result.show();

        transactionDF.createOrReplaceTempView("transactions");

        String query="Select bankName,COUNT(*) as TotalTransactions FROM transactions GROUP BY bankName";

        Dataset<Row> resultSQL=spark.sql(query);
        resultSQL.show();

        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }
}
