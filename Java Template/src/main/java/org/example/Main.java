package org.example;

import org.apache.commons.math3.util.Precision;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import org.apache.spark.sql.Dataset;
import scala.Tuple3;
import scala.Tuple4;

import java.util.ArrayList;
import java.util.List;


public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getRootLogger().setLevel(Level.OFF);

        boolean onServer = false; // TODO: Set this to true if and only if building a JAR to run on the server

        SparkConf conf = new SparkConf()
                .setAppName(Main.class.getName());
        if (!onServer) conf = conf.setMaster("local[*]");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            SparkSession spark = SparkSession
                    .builder()
                    .appName("2AMD15")
                    .getOrCreate();

            String path = (onServer) ? "/plays.txt" : "plays.txt";

            // Question 0: Load the data as an RDD and as a DataFrame
            JavaRDD<String> rddRatings = null;
            Dataset<Row> df1 = null;
//        Your code for question 0 goes here
            rddRatings = spark.read().textFile(path).javaRDD(); // Load the file at path as a textfile into an RDD
            //df1 = spark.createDataFrame(rddRatings, String.class); // Create a dataframe from the RDD

            // ******

            // The schema is encoded in a string
            String schemaString = "user song rating";

            // Generate the schema based on the string of schema
            List<StructField> fields = new ArrayList<>();
            for (String fieldName : schemaString.split(" ")) {
                StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
                fields.add(field);
            }
            StructType schema = DataTypes.createStructType(fields);

            // Convert records of the RDD (people) to Rows
            JavaRDD<Row> rowRDD = rddRatings.map((Function<String, Row>) record -> {
                String[] attributes = record.split(",");

                if(attributes.length < 3)
                    return RowFactory.create(attributes[0], attributes[1], "");
                else
                    return RowFactory.create(attributes[0], attributes[1], attributes[2]);
            });

            // Apply the schema to the RDD
            df1 = spark.createDataFrame(rowRDD, schema);

            // ******
            //df1 = spark.read().textFile(path).toDF(); // Load the file at path as a textfile into a Dataframe
            df1.show();
            // Question 0 END

            long startTime = System.currentTimeMillis();
            long endTime;
            long ans1 = question1.solution(spark, df1);
            System.out.println(">> Q1: " + ans1);
            endTime = System.currentTimeMillis();
            System.out.println("Q1 time: " + (endTime - startTime) + "ms");

            startTime = System.currentTimeMillis();
            Tuple2<Integer, Float> ans2 = question2.solution(spark, rddRatings);
            System.out.println(">> Q2: " + ans2._1 + ", " + ans2._2);
            endTime = System.currentTimeMillis();
            System.out.println("Q2 time: " + (endTime - startTime) + "ms");

            startTime = System.currentTimeMillis();
            for (Tuple3<Integer, Integer, Double> t : question3.solution(spark, rddRatings)) {
                System.out.println(">> Q3: <" + t._1() + ", " + t._2() + ", " + Precision.round(t._3(), 2) + ">");
            }
            endTime = System.currentTimeMillis();
            System.out.println("Q3 time: " + (endTime - startTime) + "ms");

            startTime = System.currentTimeMillis();
            System.out.println(">> Q4: " + question4.estimate(spark, rddRatings));
            endTime = System.currentTimeMillis();
            System.out.println("Q4 time: " + (endTime - startTime) + "ms");

            startTime = System.currentTimeMillis();
            for (Tuple4<Integer, Integer, Integer, Integer> t : question5.solution(spark, rddRatings)) {
                System.out.println(">> Q5: <" + t._1() + "," + t._2() + "," + t._3() + "," + t._4() + ">");
            }
            endTime = System.currentTimeMillis();
            System.out.println("Q5 time: " + (endTime - startTime) + "ms");
        }
    }
}