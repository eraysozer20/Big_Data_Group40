package org.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.In;
import scala.Array;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class question4 {
    public static int estimate(SparkSession spark, JavaRDD<String> rddRatings) {
        int ans=0;
        // your code goes here
        Random rand = new Random(3043043);
        // Calculate # of repetitions r:
        float epsilon = 0.1f;
        float delta = 0.1f;
        int r = (int) Math.ceil((1/Math.pow(epsilon, 2)) * (Math.log10(1/delta)/Math.log10(2)));

        int logM = 31; // Song ids are at most 31 bits

        ArrayList<Tuple2<Integer, Integer>> hashes = new ArrayList<>(r);

        for(int i = 0; i < r; i++){
            int random_odd = rand.nextInt(Integer.MAX_VALUE) | 1;
            int random_number = rand.nextInt(Integer.MAX_VALUE);

            hashes.add(new Tuple2<>(random_odd, random_number));
        }

        // We have "userId, songId, rating", map to (userId, songId)
        JavaRDD<Tuple2<Integer, Integer>> rddRatingsAsTuples = rddRatings.map(s -> {
            String[] cut = s.split(",");
            return new Tuple2<>(Integer.parseInt(cut[0]), Integer.parseInt(cut[1]));
        });
        // Map to (userId, r*[0,.....,0]) sketches
        JavaPairRDD<Integer, ArrayList<Integer>> rddSongToSketches = rddRatingsAsTuples.mapToPair((view) ->{
            ArrayList<Integer> sketches = new ArrayList<>();
            // r repetitions
            for(int i = 0; i < r; i++){
                int sketch_i = 0; // first 31 bits keep track of the sketch

                int song_hashed = hash(view._2(), hashes.get(i));

                int lsb_index = 32;

                // Just find the first bit that is 1...
                for(int bit = 0; bit < 31; bit++){
                    if((song_hashed & (1 << bit)) != 0){
                        lsb_index = bit;
                        break;
                    }
                }

                if(lsb_index < 31){
                    sketch_i = sketch_i | (1 << lsb_index);
                }

                sketches.add(sketch_i);
            }

            return new Tuple2<>(view._1(), sketches);
        });
        // Reduce by key, we get (sketches1, sketches2) return the combined sketches per user
        JavaPairRDD<Integer, ArrayList<Integer>> rddUserSketches = rddSongToSketches.reduceByKey((sketchesPair1, sketchesPair2) ->{
            ArrayList<Integer> sketchesCombined = new ArrayList<>(r);
            for(int i = 0; i < r; i++){
                int sketch_i_combined = sketchesPair1.get(i) | sketchesPair2.get(i);

                sketchesCombined.add(sketch_i_combined);
            }

            return sketchesCombined;
        });
        // We now have (userId, sketches_per_user), reduce to get the final sketch
        ArrayList<Integer> finalSketch = rddUserSketches.reduce((user1, user2) -> {
            ArrayList<Integer> sketchesCombined = new ArrayList<>(r);
            for(int i = 0; i < r; i++){
                int sketch_i_combined = user1._2().get(i) | user2._2().get(i);
                sketchesCombined.add(sketch_i_combined);
            }

            return new Tuple2<>(0, sketchesCombined);
        })._2();

        // R stuff
        int sum_zeroes = 0;
        for(int i = 0; i < r; i++){
            int left_most_zero = -1;
            int sketch = finalSketch.get(i);
            for(int m = logM -1 ; m >= 0; m--){
                if((sketch & (1 << m)) == 0){
                    left_most_zero = m;
                }
            }
            if(left_most_zero > 0){
                sum_zeroes += left_most_zero;
            }
        }

        ans = (int) Math.ceil(1.2928 * Math.pow(2, (float)sum_zeroes/r));

        return ans;
    }

    private static int hash(int val, Tuple2<Integer, Integer> seed) {
        val = ((val*seed._1()) + seed._2()) & 0x7FFFFFFF;
        val ^= (val>>15);
        val ^= (val>>8);

        //System.out.println(val);

        return val;
    }
}
