package com.codewithz.casestudy;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;

public class EmployeeRetirementCheck {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Employee Retirement Check")
                .master("local[*]")
                .getOrCreate();

        // Load the CSV file
        Dataset<Row> employeeDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("J:\\Zartab\\CodeWithZAcademy\\Spark\\new-data\\retirement\\sample_employee_data.csv");

        // Add current date column
        employeeDF = employeeDF.withColumn("current_date", current_date());

        // Calculate retirement eligibility within 3 months
        employeeDF = employeeDF.withColumn("retiring_soon",
                expr("date_of_retirement BETWEEN current_date() AND add_months(current_date(), 3)"));

        // Update status based on retirement eligibility
        employeeDF = employeeDF.withColumn("updated_status",
                when(col("retiring_soon").equalTo(true), "Blocked")
                        .otherwise(col("status")));

        // Filter by departments and write to separate CSV files
        String[] departments = {"IT", "Finance", "HR"};
        for (String department : departments) {
            employeeDF.filter(col("dept").equalTo(department))
                    .drop("current_date", "retiring_soon") // Drop temporary columns
                    .write()
                    .option("header", "true")
                    .csv("J:\\Zartab\\CodeWithZAcademy\\Spark\\new-data\\retirement\\output\\" + department + "_employees.csv");
        }

        spark.stop();
    }
}
