package org.example;

import cats.kernel.Hash;
import org.apache.avro.generic.GenericData;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.In;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

public class question3 {

    public static List<Tuple3<Integer, Integer, Double>> solution(SparkSession spark, JavaRDD<String> rddRatings) {
        List<Tuple3<Integer, Integer, Double>> ans = null;
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

        // Map every entry <userId, songId, rating> to a pair <userId, <map(song), rating^2>>
        JavaPairRDD<Integer, Tuple2<HashMap<Integer, Integer>, Long>> rddRatingsAsMaps = rddActualRatingsAsTuples.mapToPair((tuple) -> {
            HashMap<Integer, Integer> ratingList = new HashMap<>();
            ratingList.put(tuple._2(), tuple._3());
            return new Tuple2<>(tuple._1(), new Tuple2<>(ratingList, (long) tuple._3()* tuple._3()));
        });

        // Reduce by key, i.e. merge the hashmaps such that for each user we get a map song:rating for all songs that
        // the user rated and sum the ratings^2
        // Also persist this RDD, since we are going to do a cartesian with itself, and don't want to recompute it
        JavaPairRDD<Integer, Tuple2<HashMap<Integer, Integer>, Long>> rddRatingsPerUser = rddRatingsAsMaps.reduceByKey((ratings1, ratings2) ->{
            ratings1._1().putAll(ratings2._1());
            return new Tuple2<>(ratings1._1(), ratings1._2() + ratings2._2());
        }).persist(StorageLevel.MEMORY_AND_DISK());

        // Cartesian with itself, we get (user1,map1)(user2, map2) pairs, filter, so we have user1 < user2
        JavaPairRDD<Tuple2<Integer, Tuple2<HashMap<Integer, Integer>, Long>>, Tuple2<Integer, Tuple2<HashMap<Integer, Integer>, Long>>> rddCart = rddRatingsPerUser.cartesian(rddRatingsPerUser)
                .filter((userPair) -> userPair._1()._1() < userPair._2()._1());

        //
        JavaPairRDD<Integer, Tuple2<Integer, Double>> rddSim = rddCart.mapToPair((userPair) ->{
            // Get the song maps from each of the users
            Tuple2<HashMap<Integer, Integer>, Long> user1 = userPair._1()._2();
            Tuple2<HashMap<Integer, Integer>, Long> user2 = userPair._2()._2();

            // Find the user with the fewest rated songs
            if(user1._1().keySet().size() > user2._1().keySet().size()){
                Tuple2<HashMap<Integer, Integer>, Long> temp = user1;
                user1 = user2;
                user2 = temp;
            }

            // Calculate sum (a*b) with a: rating user1, b: rating user2 for all songs in common
            long rating_sum = 0;
            for(Map.Entry<Integer, Integer> rating : user1._1().entrySet()){
                if(user2._1().containsKey(rating.getKey())){
                    rating_sum += (long) rating.getValue() * user2._1().get(rating.getKey());
                }
            }

            // Calculate the similarity
            double sim = rating_sum/(Math.sqrt(user1._2())*Math.sqrt(user2._2()));

            // Return <user1, <user2, sim>>
            return new Tuple2<>(userPair._1()._1(), new Tuple2<>(userPair._2()._1(), sim));
        });

        // Now reduce by key to get for each user the pair <user2, sim> with maximum similarity score
        // If sim1 > sim2 return user1, if sim1==sim2 return user1 if user1 < user2, otherwise return user2
        JavaPairRDD<Integer, Tuple2<Integer, Double>> rddSimMax = rddSim.reduceByKey((sim1, sim2) ->
                sim1._2() > sim2._2() ? sim1 : (Objects.equals(sim1._2(), sim2._2()) ?
                        (sim1._1() < sim2._1() ? sim1 : sim2) : sim2));

        // Finally, map to <user1, user2, similarity>
        JavaRDD<Tuple3<Integer, Integer, Double>> rddSimMaxRounded = rddSimMax.map((sim) ->
                new Tuple3<>(sim._1(), sim._2()._1(), sim._2()._2()));

        // Now collect the results
        ans = rddSimMaxRounded.collect();

        rddRatingsPerUser.unpersist();

        return ans;
    }

}
