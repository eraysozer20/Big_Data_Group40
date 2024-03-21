package org.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.util.HashMap;
import java.util.List;
import java.util.Objects;

public class question3 {

    public static List<Tuple3<Integer, Integer, Double>> solution(SparkSession spark, JavaRDD<String> rddRatings) {
        List<Tuple3<Integer, Integer, Double>> ans = null;
        // your code goes here

        // First map to a Tuple3 of the form user (int), song (int), rating/-1.0f (float)
        JavaRDD<Tuple3<Integer, Integer, Integer>> rddRatingsAsTuples = rddRatings.map(s -> {String[] cut = s.split(","); return new Tuple3<>(Integer.parseInt(cut[0]), Integer.parseInt(cut[1]), cut.length == 3 ? Integer.parseInt(cut[2]) : -1);});
        // Remove all entries that did not have a proper rating
        JavaRDD<Tuple3<Integer,Integer,Integer>> rddActualRatingsAsTuples = rddRatingsAsTuples.filter(r -> r._3() > 0.0f);
        
        JavaPairRDD<Integer, Tuple2<HashMap<Integer, Integer>, Integer>> rddRatingsAsMaps = rddActualRatingsAsTuples.mapToPair((tuple) -> {
            HashMap<Integer, Integer> ratingsMap = new HashMap<>();
            ratingsMap.put(tuple._2(), tuple._3());
            return new Tuple2<>(tuple._1(), new Tuple2<>(ratingsMap, (int)Math.pow(tuple._3(),2)));
        });

        JavaPairRDD<Integer, Tuple2<HashMap<Integer, Integer>, Integer>> rddRatingsPerUser = rddRatingsAsMaps.reduceByKey((ratings1, ratings2) ->{
            ratings1._1().putAll(ratings2._1());
            return new Tuple2<>(ratings1._1(), ratings1._2() + ratings2._2());
        });

        JavaPairRDD<Tuple2<Integer, Tuple2<HashMap<Integer, Integer>, Integer>>, Tuple2<Integer, Tuple2<HashMap<Integer, Integer>, Integer>>> rddCart = rddRatingsPerUser.cartesian(rddRatingsPerUser).filter((tuple) -> tuple._1()._1() < tuple._2()._1()).cache();

        JavaPairRDD<Integer, Tuple2<Integer, Double>> rddSimilarity = rddCart.mapToPair((tuple) -> {
            Tuple2<Integer, Tuple2<HashMap<Integer, Integer>, Integer>> user1 = tuple._1();
            Tuple2<Integer, Tuple2<HashMap<Integer, Integer>, Integer>> user2 = tuple._1();

            if(user1._2()._1().keySet().size() > user2._2()._1().keySet().size()){
                Tuple2<Integer, Tuple2<HashMap<Integer, Integer>, Integer>> temp = user1;
                user1 = user2;
                user2 = temp;
            }

            long sum = 0;

            for(Integer song : user1._2()._1().keySet()){
                if(user2._2()._1().containsKey(song)){
                    sum += (long) user1._2()._1().get(song) * user2._2()._1().get(song);
                }
            }

            double sim = sum / (Math.sqrt(user1._2()._2())*Math.sqrt(user2._2()._2()));

            return new Tuple2<>(tuple._1()._1(), new Tuple2<>(tuple._2()._1(), sim));
        }).filter((userPair) -> userPair._2()._2() > 0.95);

        JavaPairRDD<Integer, Tuple2<Integer, Double>> rddSimMax = rddSimilarity.reduceByKey((sim1, sim2) -> sim1._2() > sim2._2() ? sim1 : (Objects.equals(sim1._2(), sim2._2()) ? (sim1._1() < sim2._1() ? sim1 : sim2) : sim2));
        JavaRDD<Tuple3<Integer, Integer, Double>> rddSimMaxRounded = rddSimMax.map((sim) -> new Tuple3<>(sim._1(), sim._2()._1(), sim._2()._2()));

        ans = rddSimMaxRounded.collect();

        return ans;
    }

}
