package com.mady;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class ReadCsvIntoDataSet {

    public static void main(String[] args) {
        try (
            SparkSession session =  SparkSession.builder()
                    .appName("ReadCsv")
                    .master("local")
                    .getOrCreate()
        ){
            Dataset<Row> employees = session.read()
                    .option("delimiter",";")
                    .option("header",true)
                    .option("inferSchema",true)
                    .csv("src/main/resources/employee.csv");

            employees.printSchema();
            employees.show();

            employees.select(col("name"),col("job")).show();



        }


    }
}
