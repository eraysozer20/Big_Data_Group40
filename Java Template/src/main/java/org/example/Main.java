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
import org.sparkproject.guava.base.Functions;
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

        boolean onServer = true; // TODO: Set this to true if and only if building a JAR to run on the server

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
            // Q0b
            // Load the file at path as a textfile into an RDD and set the minimum number of partitions
            rddRatings = sc.textFile(path, 40);

            // Q0a
            // Define a schema for the values in our text file
            List<StructField> fields = new ArrayList<>();
            fields.add(DataTypes.createStructField("userId", DataTypes.IntegerType, true));
            fields.add(DataTypes.createStructField("songId", DataTypes.IntegerType, true));
            fields.add(DataTypes.createStructField("rating", DataTypes.IntegerType, true));
            StructType schema = DataTypes.createStructType(fields);

            // Convert the string into a (userId, songId, rating) row
            // If there was no rating, set it to -1
            JavaRDD<Row> rowRDD = rddRatings.map((Function<String, Row>) record -> {
                String[] ratingSplit = record.split(",");
                return RowFactory.create(Integer.parseInt(ratingSplit[0]), Integer.parseInt(ratingSplit[1]), ratingSplit.length > 2 ? Integer.parseInt(ratingSplit[2]): -1);
            });

            // Convert the RDD into a dataframe using the schema
            df1 = spark.createDataFrame(rowRDD, schema);
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