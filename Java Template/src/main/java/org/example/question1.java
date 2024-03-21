package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class question1 {
    public static long solution(SparkSession spark, Dataset<Row> df1){
        long ans=0;
        // Your code goes here
        // Temporary table that maps string to user, song, rating
        df1.createOrReplaceTempView("ratings_table");
        // Need to grab only the user
        // Need to consider only rows with actual rating, > 0
        // Need on a per-user basis -> need to group by user
        // Need that # of ratings >= 100 and that avg rating < 2
        // Finally only need the # of users that satisfy this query
        ans = spark.sql("SELECT userId " +
                                "FROM ratings_table " +
                                "WHERE rating > 0 " +
                                "GROUP BY userId " +
                                "HAVING COUNT(songId) >= 100 AND AVG(rating) < 2").count();

        return ans;
    }
}
