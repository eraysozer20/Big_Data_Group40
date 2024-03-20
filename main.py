from timeit import timeit

from pyspark import SparkConf, SparkContext, RDD, StorageLevel
from pyspark.sql import SparkSession, DataFrame

# The code below may help you if your pc cannot find the correct python executable.
# Don't use this code on the server!
# import os
# import sys
# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# TODO: Make sure that if you installed Spark version 3.3.4 (recommended) that you install the same version of
#  PySpark. You can do this by running the following command: pip install pyspark==3.3.4


def get_spark_context(on_server: bool) -> SparkContext:
    spark_conf = SparkConf().setAppName("2AMD15")
    if not on_server:
        spark_conf = spark_conf.setMaster("local[*]")

    spark_context = SparkContext.getOrCreate(spark_conf)

    if on_server:
        # TODO: You may want to change ERROR to WARN to receive more info. For larger data sets, to not set the
        #  log level to anything below WARN, Spark will print too much information.
        spark_context.setLogLevel("ERROR")

    spark_context.setLogLevel("ERROR") #Delete

    return spark_context


def q0a(spark_context: SparkContext, on_server: bool) -> DataFrame:
    plays_file_path = "/plays.txt" if on_server else "plays.txt"

    spark_session = SparkSession.builder.config(conf=spark_context.getConf()).getOrCreate()

    # TODO: Implement Q0a here by creating a Dataset of DataFrame out of the file at {@code plays_file_path}.
    from pyspark.sql.functions import split, col

    raw_data = spark_session.read.text(plays_file_path)

    # Split the column into userId, songId, and rating
    df = raw_data.select(
        split(col("value"), ",").getItem(0).cast("int").alias("userId"),
        split(col("value"), ",").getItem(1).cast("int").alias("songId"),
        split(col("value"), ",").getItem(2).cast("double").alias("rating")
    )

    return df


def q0b(spark_context: SparkContext, on_server: bool) -> RDD:
    plays_file_path = "/plays.txt" if on_server else "plays.txt"

    # TODO: Implement Q0b here by creating an RDD out of the file at {@code plays_file_path}.
    # Load the file as a string RDD: "userid, songid, rating"
    string_rdd = spark_context.textFile(plays_file_path)

    # Pre-process by converting it into (userid, songid, rating)
    # Also persist this RDD, since all other questions use this one as a base
    rating_rdd = string_rdd.map(
        lambda s: (int(s.split(",")[0]), int(s.split(",")[1]), int(s.split(",")[2])) if len(s.split(",")) > 2 else (
            int(s.split(",")[0]), int(s.split(",")[1]), -1)).persist(storageLevel=StorageLevel.MEMORY_AND_DISK)

    return rating_rdd

def q1(spark_context: SparkContext, data_frame: DataFrame):
    # TODO: Implement Q1 here
    spark = SparkSession(spark_context)

    data_frame.createOrReplaceTempView("ratings_table")
    result = spark.sql("""
    SELECT userId
    FROM ratings_table
    WHERE rating IS NOT NULL
    GROUP BY userId
    HAVING COUNT(songId) >= 100 AND AVG(rating) < 2""")

    print("Q1: ", result.count())
    
    return result


def q2(spark_context: SparkContext, rdd: RDD):

    # Filter out entries without a rating (-1)
    rdd_ratings_only = rdd.filter(lambda record: record[2] > 0)

    # Map (userId, songId, rating) to (userId, (1, rating))
    rdd_ratings_count = rdd_ratings_only.map(lambda record: (record[0], (1, record[2])))

    # Reduce by key to get for each user the # of ratings and the sum of all ratings
    # I.e. we get (userId, (count, sum_ratings)
    rdd_ratings_by_user = rdd_ratings_count.reduceByKey(lambda ratings1, ratings2: (ratings1[0] + ratings2[0], ratings1[1] + ratings2[1]))

    # Now filter only those who have given at least 100 ratings
    rdd_ratings_by_user_at_least100 = rdd_ratings_by_user.filter(lambda user: user[1][0] >= 100)

    # Now map (userId, (count, sum)) to (userId, average_rating)
    rdd_average_rating_by_user_at_least100 = rdd_ratings_by_user_at_least100.map(lambda r1: (r1[0], r1[1][1] / r1[1][0]))

    # Finally, reduce to find the grumpiest user
    ans = rdd_average_rating_by_user_at_least100.reduce(lambda r1, r2: r1 if r1[1] < r2[1] else r2)

    print("Q2: ", ans[0], ", ", ans[1])

    return ans


def q3(spark_context: SparkContext, rdd: RDD):
    # TODO: Implement Q3 here

    # Filter out entries without a rating (-1)
    rdd_ratings_only = rdd.filter(lambda record: record[2] > 0)

    # Map every entry (userId, songId, rating) to (userId, ({songId: rating}, rating**2))
    rdd_songList = rdd_ratings_only.map(lambda record: (record[0], ({record[1]: record[2]}, record[2] ** 2)))

    # A helper function that merges two maps/dictionaries and returns the first
    def map_merge(map1, map2):
        map1.update(map2)
        return map1

    # Reduce by key, i.e. merge the dictionaries such that for each user we get a {songId: rating} dictionary and sum of rating**
    # Also, persist this RDD to prevent recomputing it in the cartesian
    rdd_ratings_per_user = rdd_songList.reduceByKey(
        lambda ratings1, ratings2: (map_merge(ratings1[0], ratings2[0]), ratings1[1] + ratings2[1])).persist(storageLevel=StorageLevel.MEMORY_AND_DISK)

    # Now perform the cartesian with itself to create all user pairs and their ratings, filter such that we only
    # get each pair once since we don't care about order within a pair
    rdd_cart = rdd_ratings_per_user.cartesian(rdd_ratings_per_user).filter(
        lambda pair: pair[0][0] < pair[1][0])

    # Helper function to compute the similarity between two users
    def similarity(user_pair):
        import math

        # Split the pair into two users
        user1 = user_pair[0]
        user2 = user_pair[1]

        # Find the user in this pair with the smallest song list
        if len(user1[1][0].keys()) > len(user2[1][0].keys()):
            temp = user1
            user1 = user2
            user2 = temp

        ratings_sum = 0
        # For each song in the list, check if the other user has it in common and if so sum the product
        for song in user1[1][0].keys():
            if song in user2[1][0]:
                ratings_sum += user1[1][0][song] * user2[1][0][song]

        # Finally calculate the similarity score, by dividing ratings_sum by the sqrt of each of the individual ratings
        sim = ratings_sum / (math.sqrt(user1[1][1]) * math.sqrt(user2[1][1]))
        return sim

    # Map the pair of users to (user1, (user2, similarity)) and filter such that only pairs with sim > 0.95 remain
    rdd_sim = rdd_cart.map(lambda user_pair: (user_pair[0][0], (user_pair[1][0], similarity(user_pair)))).filter(
        lambda similarities: similarities[1][1] > 0.95)

    # Helper function that compares two (user, similarity) pairs and returns the maximum
    def maxSim(similarity1, similarity2):
        # If the first one has a higher similarity than the second, return the first
        if similarity1[1] > similarity2[1]:
            return similarity1
        # If they have equal similarity, return the one with the smallest userId
        elif similarity1[1] == similarity2[1]:
            if similarity1[0] < similarity2[0]:
                return similarity1
            else:
                return similarity2
        # Otherwise, return the second
        else:
            return similarity2

    # Now reduce by key to get for each user another user with the highest similarity score
    rdd_sim_max = rdd_sim.reduceByKey(lambda b1, b2: maxSim(b1, b2))

    # Now print all the results
    result = rdd_sim_max.collect()

    [print("Q3: <", sim[0], ", ", sim[1][0], ", ", round(sim[1][1], 2), ">") for sim in result]

    # Done with this question, so unpersist this RDD
    rdd_ratings_per_user.unpersist()

    return


def q4(spark_context: SparkContext, rdd: RDD):
    # TODO: Implement Q4 here
    return


def q5(spark_context: SparkContext, rdd: RDD):
    # TODO: Implement Q5 here
    return


if __name__ == '__main__':
    on_server = False  # TODO: Set this to true if and only if deploying to the server

    spark_context = get_spark_context(on_server)

    data_frame = q0a(spark_context, on_server)

    rdd = q0b(spark_context, on_server)

    print("Q1 took: ", timeit(lambda: q1(spark_context, data_frame), number=1))

    print("Q2 took: ", timeit(lambda: q2(spark_context, rdd), number=1))

    #print("Q3 took: ", timeit(lambda: q3(spark_context, rdd), number=1))

    q4(spark_context, rdd)

    q5(spark_context, rdd)

    spark_context.stop()
