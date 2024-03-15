from timeit import timeit

from pyspark import SparkConf, SparkContext, RDD, StorageLevel
from pyspark.mllib.linalg import SparseVector
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

    spark_context.setLogLevel("ERROR")  # Delete

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
    string_rdd = spark_context.textFile(plays_file_path)

    rating_rdd = string_rdd.map(
        lambda s: (int(s.split(",")[0]), int(s.split(",")[1]), int(s.split(",")[2])) if len(s.split(",")) > 2 else (
            int(s.split(",")[0]), int(s.split(",")[1]), -1))#.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)

    # [print(i) for i in rating_rdd.take(10)]

    return rating_rdd


def q1(spark_context: SparkContext, data_frame: DataFrame):
    # TODO: Implement Q1 here
    spark = SparkSession(spark_context)

    data_frame.createOrReplaceTempView("ratings_table")
    # result = spark.sql("""
    # SELECT userId, COUNT(DISTINCT songId) as total_ratings, AVG(rating) as avg_rating
    # FROM ratings_table
    # WHERE rating IS NOT NULL
    # GROUP BY userId
    # HAVING total_ratings >= 100 AND avg_rating < 2""")

    result = spark.sql("""
    SELECT MAX(songId)
    FROM ratings_table
    """)

    print(result.show())  # Delete

    return result


def q2(spark_context: SparkContext, rdd: RDD):
    return


def q3(spark_context: SparkContext, rdd: RDD):
    # TODO: Implement Q3 here
    import time

    print("working on q3")

    # Remove None values
    rdd_notnone = rdd.filter(lambda record: record[2] > 0)

    # Map every entry (userId, songId, rating) into (userId, {songId: rating}, rating**2 )
    rdd_songList = rdd_notnone.map(lambda record: (record[0], ({record[1]: record[2]}, record[2] ** 2)))

    def map_merge(map1, map2):
        map1.update(map2)
        return map1

    # Reduce by key, i.e. merge the dictionaries such that for each user we get a {songId: rating} dictionary and sum of rating**
    rdd_ratings_per_user = rdd_songList.reduceByKey(
        lambda ratings1, ratings2: (map_merge(ratings1[0], ratings2[0]), ratings1[1] + ratings2[1])).persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
    # [print("ratings ", i) for i in rdd_ratings_per_user.take(1)]

    rdd_cart = rdd_ratings_per_user.cartesian(rdd_ratings_per_user).filter(
        lambda pair: pair[0][0] < pair[1][0])

    # [print("card ", i) for i in rdd_cart.take(1)]

    def similarity(user_pair):
        # user_pair = ((user1, ({songId, rating}, total_rating_sqr)), (user2, ({songId, rating}, total_rating_sqr))
        # {songId, rating} o {songId, rating} => {songCommon, (rating1, rating2)}
        # foreach songCommmon
        # do the math

        import math

        user1 = user_pair[0]
        user2 = user_pair[1]

        # Find songs in common by converting one of the user's song list (the smallest) into a set and
        # using intersection with the other user's song list
        songs = None
        if len(user1[1][0].keys()) > len(user2[1][0].keys()):
            temp = user1
            user1 = user2
            user2 = temp

        ratings_sum = 0
        for song in user1[1][0].keys():
            if song in user2[1][0]:
                ratings_sum += user1[1][0][song] * user2[1][0][song]

        sim = ratings_sum / (math.sqrt(user1[1][1]) * math.sqrt(user2[1][1]))
        return sim

    rdd_sim = rdd_cart.map(lambda user_pair: (user_pair[0][0], (user_pair[1][0], similarity(user_pair)))).filter(
        lambda similarities: similarities[1][1] > 0.95)

    # [print("sim", i) for i in rdd_sim.take(1)]

    def maxSim(b1, b2):
        if b1[1] > b2[1]:
            return b1
        elif b1[1] == b2[1]:
            if b1[0] < b2[0]:
                return b1
            else:
                return b2
        else:
            return b2

    rdd_sim_max = rdd_sim.reduceByKey(lambda b1, b2: maxSim(b1, b2))
    # [print("sim max ", i) for i in rdd_sim_max.take(1)]

    rdd_sim_max_rounded = rdd_sim_max.map(lambda sim: (sim[0], sim[1][0], round(sim[1][1], 2)))
    # [print("sim max rounded", i) for i in rdd_sim_max_rounded.take(rdd_sim_max_rounded.count())]

    print(rdd_sim_max_rounded.collect())

    return


def q3_alt(spark_context: SparkContext, rdd: RDD):
    # TODO: Implement Q3 here
    print("working on q3")

    # Remove None values
    rdd_notnone = rdd.filter(lambda record: record[2] > 0)

    # Map every entry (userId, songId, rating) into (user, [(songid, rating)]
    rdd_songList = rdd_notnone.map(lambda record: (record[0], {record[1]: record[2]}))

    # Up to this point is fast: ~3s
    def list_merge(list1, list2):
        list1.append(list2)
        return list1

    # Reduce by key, i.e. merge the lists
    rdd_ratings_per_user = rdd_songList.reduceByKey(
        lambda ratings1, ratings2: {**ratings1, **ratings2})
    #[print("ratings ", i) for i in rdd_ratings_per_user.take(1)]

    # Convert the list into sparsevectors (user, SparseVector)
    rdd_ratings_per_user_as_vector = rdd_ratings_per_user.map(
        lambda user_rating: (user_rating[0], SparseVector(50947+1, user_rating[1])))

    # [print("toVector", i) for i in rdd_ratings_per_user_as_vector.take(1)]
    #print("ToVector", rdd_ratings_per_user_as_vector.count())

    # Now do the cartesian, try persistence to see if it speeds up
    rdd_cart = rdd_ratings_per_user_as_vector.cartesian(rdd_ratings_per_user_as_vector).filter(
        lambda pair: pair[0][0] < pair[1][0]).persist(storageLevel=StorageLevel.MEMORY_AND_DISK)

    #[print("cart", i) for i in rdd_cart.take(1)]

    # Map (user1, SparseVector)(user2, SparseVector) to (user1, (user2, similarity))
    rdd_sim = rdd_cart.map(
        lambda user_pair: (user_pair[0][0], (user_pair[1][0], user_pair[0][1].dot(user_pair[1][1] / (user_pair[0][1].norm(2) * user_pair[1][1].norm(2)))))
    )

    #print("sim ", rdd_sim.take(1))

    def maxSim(b1, b2):
        if b1[1] > b2[1]:
            return b1
        elif b1[1] == b2[1]:
            if b1[0] < b2[0]:
                return b1
            else:
                return b2
        else:
            return b2

    rdd_sim_filtered = rdd_sim.filter(lambda similarities: similarities[1][1] > 0.95)
    [print("sim filtered ", i) for i in rdd_sim_filtered.take(1)]

    #rdd_sim_max = rdd_sim_filtered.reduceByKey(lambda b1, b2: maxSim(b1, b2))
    #[print("sim max ", i) for i in rdd_sim_max.take(1)]

    #rdd_sim_max_rounded = rdd_sim_max.map(lambda sim: (sim[0], sim[1][0], round(sim[1][1], 2)))
    #[print("sim max rounded", i) for i in rdd_sim_max_rounded.take(rdd_sim_max_rounded.count())]

    #print(rdd_sim_max_rounded.take(1))

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
    print(type(data_frame))  # Delete
    print(data_frame.show())  # Delete

    rdd = q0b(spark_context, on_server)
    #print(type(rdd))  # Delete
    #print('Here: ' + str(rdd.count()))  # Delete

    #q1(spark_context, data_frame)

    # q2(spark_context, rdd)

    print("Took: ", timeit(lambda: q3(spark_context, rdd), number=1))

    q4(spark_context, rdd)

    q5(spark_context, rdd)

    spark_context.stop()
