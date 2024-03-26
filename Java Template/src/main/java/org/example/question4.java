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
        int ans = 0;

        // your code goes here
        // Create a random, with some predefined seed, so we always get the same results for the same epsilon and delta
        Random rand = new Random(3043043);

        // Calculate # of repetitions r for epsilon and delta:
        float epsilon = 0.1f;
        float delta = 0.1f;
        int r = (int) Math.ceil((1 / Math.pow(epsilon, 2)) * (Math.log10(1 / delta) / Math.log10(2)));

        int logM = 31; // Song ids are at most 31 bits

        // For each hash function we need two random numbers, one odd, one whatever
        ArrayList<Tuple2<Integer, Integer>> hashes = new ArrayList<>(r);
        for (int i = 0; i < r; i++) {
            int random_odd = rand.nextInt(Integer.MAX_VALUE) | 1; // Generate a random positive int and make it odd if it wasn't
            int random_number = rand.nextInt(Integer.MAX_VALUE);

            // The parameters for this hash function
            hashes.add(new Tuple2<>(random_odd, random_number));
        }

        // We have "userId, songId, rating", map to songId since we don't need anything else
        JavaRDD<Integer> rddRatingsAsTuples = rddRatings.map(s -> {
            String[] cut = s.split(",");
            return Integer.parseInt(cut[1]);
        });

        // Map to (r*[0,.....,0]) sketches
        JavaRDD<ArrayList<Integer>> rddSongToSketches = rddRatingsAsTuples.map((song) -> {
            ArrayList<Integer> sketches = new ArrayList<>();
            // r repetitions
            for (int i = 0; i < r; i++) {
                int sketch_i = 0; // first 31 bits keep track of the sketch

                // Hash the song id using the ith hash function
                int song_hashed = hash(song, hashes.get(i));

                // Just find the first bit that is 1...
                int lsb_index = 32;
                for (int bit = 0; bit < 31; bit++) {
                    if ((song_hashed & (1 << bit)) != 0) {
                        lsb_index = bit;
                        break;
                    }
                }

                if (lsb_index < 31) {
                    // Set the lsb_index bit to 1
                    sketch_i = sketch_i | (1 << lsb_index);
                }

                sketches.add(sketch_i);
            }

            return sketches;
        });

        // We now have (userId, sketches_per_user), reduce to get the final sketch
        ArrayList<Integer> finalSketch = rddSongToSketches.reduce((song1, song2) -> {
            ArrayList<Integer> sketchesCombined = new ArrayList<>(r);
            for (int i = 0; i < r; i++) {
                int sketch_i_combined = song1.get(i) | song2.get(i);
                sketchesCombined.add(sketch_i_combined);
            }

            return sketchesCombined;
        });

        // R stuff
        int sum_zeroes = 0;
        // For each sketch find the position of the left-most zero
        for (int i = 0; i < r; i++) {
            int left_most_zero = -1;
            int sketch = finalSketch.get(i);
            for (int m = logM - 1; m >= 0; m--) {
                if ((sketch & (1 << m)) == 0) {
                    left_most_zero = m;
                }
            }

            // If there is any, add the position to the sum
            if (left_most_zero > 0) {
                sum_zeroes += left_most_zero;
            }
        }

        // Finally calculate the estimate
        ans = (int) Math.ceil(1.2928 * Math.pow(2, (float) sum_zeroes / r));

        return ans;
    }

    // Fast bijective 31-bit hash-function by Matt Timmermans @ StackOverflow
    // https://stackoverflow.com/questions/55957869/31-bit-bijective-perfect-hash-algorithm
    private static int hash(int val, Tuple2<Integer, Integer> seed) {
        val = ((val * seed._1()) + seed._2()) & 0x7FFFFFFF;
        val ^= (val >> 15);
        val ^= (val >> 8);

        return val;
    }
}
