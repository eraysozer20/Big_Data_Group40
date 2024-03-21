package org.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;
import scala.Tuple3;

public class question2 {
    public static Tuple2<Integer,Float> solution(SparkSession spark,  JavaRDD<String> rddRatings){
        Tuple2<Integer,Float> ans = null;
        // your code goes here

        // Start with a string in the form of user,song,(rating)
        // First map to a Tuple3 of the form user (int), song (int), rating/-1
        JavaRDD<Tuple3<Integer, Integer, Integer>> rddRatingsAsTuples = rddRatings.map(s -> {
            String[] cut = s.split(",");
            return new Tuple3<>(Integer.parseInt(cut[0]), Integer.parseInt(cut[1]), cut.length == 3 ? Integer.parseInt(cut[2]) : -1);
        });
        // Remove all entries that did not have a proper rating
        JavaRDD<Tuple3<Integer,Integer,Integer>> rddActualRatingsAsTuples = rddRatingsAsTuples.filter(r -> r._3() > 0);
        // Now we have a rdd with user (int), song (int), rating (int)
        // Now map to pair such that we have <user, <1, rating>>
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> rddRatingCounts = rddActualRatingsAsTuples.mapToPair(r -> new Tuple2<>(r._1(), new Tuple2<>(1, r._3())));
        // Sum the count and ratings for each user, so we get <user, <count, rating_sum>>
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> rddRatingsByUser = rddRatingCounts.reduceByKey((r1,r2)-> new Tuple2<>(r1._1() + r2._1(), r1._2() + r2._2()));
        // Filter on users with count >= 100
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> rddRatingsByUserAtLeast100 = rddRatingsByUser.filter(r -> r._2()._1() >= 100);

        // Calculate the average per user -> <user, rating/count>
        JavaPairRDD<Integer,Float> rddAverageRatingByUserAtLeast100 = rddRatingsByUserAtLeast100.mapToPair(r1 -> new Tuple2<>(r1._1(), ((float)r1._2()._2())/r1._2()._1()));

        // Finally, calculate the grumpiest user
        ans = rddAverageRatingByUserAtLeast100.reduce((r1, r2) -> r1._2() < r2._2() ? r1 : r2);

        return ans;
     }
}
