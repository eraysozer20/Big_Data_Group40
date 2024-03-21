package org.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import java.util.*;

class question5 {

    public static List<Tuple4<Integer, Integer, Integer, Integer>> solution(SparkSession spark, JavaRDD<String> rddRatings) {
        List<Tuple4<Integer, Integer, Integer, Integer>> ans = null;
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

        // Map every entry <userId, songId, rating> to a pair <userId, set(song)>
        JavaPairRDD<Integer, HashSet<Integer>> rddSongSets = rddActualRatingsAsTuples.mapToPair((tuple) -> {
            HashSet<Integer> ratedSongs = new HashSet<>();
            ratedSongs.add(tuple._2());
            return new Tuple2<>(tuple._1(), ratedSongs);
        });

        // Reduce by key, i.e. merge the sets such that for each user we get a set of distinct songs (should already
        // be the case, but easier to have set when we combine with other users)
        // Also persist this RDD, since we are going to do a cartesian with itself (twice), and don't want to recompute it
        JavaPairRDD<Integer, HashSet<Integer>> rddSongsByUser = rddSongSets.reduceByKey((songs1, songs2) ->{
            // Add the smallest to the largest set
            if(songs1.size() < songs2.size()){
                songs2.addAll(songs1);
                return songs2;
            }else {
                songs1.addAll(songs2);
                return songs1;
            }
        }).filter((user)-> user._2().size() <=8000) // Can already filter out users that rated more than 8000 songs
        .persist(StorageLevel.MEMORY_ONLY());

        // Cartesian with itself, we get (user1,set1)(user2, set2) pairs, filter, so we have user1 < user2
        JavaPairRDD<Tuple2<Integer, HashSet<Integer>>, Tuple2<Integer, HashSet<Integer>>> rddCart = rddSongsByUser.cartesian(rddSongsByUser)
                .filter((userPair) -> userPair._1()._1() < userPair._2()._1());

        // Now map (user1, set1)(user2, set2) to (user1, user2, songsDistinct)
        JavaRDD<Tuple3<Integer, Integer, HashSet<Integer>>> rddUserPairs = rddCart.map((userPair)->{
            // Create a new set by copying one and adding the other
            HashSet<Integer> distinct = new HashSet<>(userPair._1()._2());
            distinct.addAll(userPair._2()._2());
            return new Tuple3<>(userPair._1()._1(), userPair._2()._1(), distinct);
        }).filter((userPair) -> userPair._3().size() <= 8000) // Again can already filter out user pairs that have > 8000 songs
        .persist(StorageLevel.MEMORY_AND_DISK());

        // Now do another cartesian to get (user1, user2, songsDistinct)(user3, songs)
        // And filter, so we have user1 < user2 < user3
        JavaPairRDD<Tuple3<Integer, Integer, HashSet<Integer>>, Tuple2<Integer, HashSet<Integer>>> rddSecondCart = rddUserPairs.cartesian(rddSongsByUser)
                .filter((userTriple) -> userTriple._1()._2() < userTriple._2()._1());

        // Now do another map, so we obtain (user1, user2, user3, #songsDistinct)
        // And filter to get only those with <= 8000 songs
        JavaRDD<Tuple4<Integer, Integer, Integer, Integer>> rddUserTriples = rddSecondCart.map((userTriple) -> {
            HashSet<Integer> distinct = new HashSet<>(userTriple._1()._3());
            distinct.addAll(userTriple._2()._2());
            return new Tuple4<>(userTriple._1()._1(), userTriple._1()._2(), userTriple._2()._1(), distinct.size());
        }).filter((userTriple) -> userTriple._4() <= 8000);

        // Finally collect the results
        ans = rddUserTriples.collect();

        rddSongsByUser.unpersist();
        rddUserPairs.unpersist();


        return ans;

    }
}
