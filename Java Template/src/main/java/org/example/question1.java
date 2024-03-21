package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class question1 {
    public static long solution(SparkSession spark, Dataset<Row> df1){
        long ans=0;
        // Your code goes here
        // Temporary table that maps string to user, song, rating
        df1.createOrReplaceTempView("ratings");
        // SELECT only rows that have rating not null
        // THEN SELECT users such that COUNT(rating) >= 100, GROUP BY user
        // THEN SELECT COUNT(DISTINCT user) WHERE AVG(rating) < 2 GROUP BY user
        ans = spark.sql("SELECT user, COUNT(user) AS total_ratings, AVG(rating) as avg_rating FROM ratings WHERE rating IS NOT NULL GROUP BY USER HAVING total_ratings >= 100 AND avg_rating < 2").count();

        return ans;
    }
}
