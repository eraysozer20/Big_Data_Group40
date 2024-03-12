from pyspark import SparkConf, SparkContext, RDD
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

    rating_rdd = string_rdd.map(lambda s: (int(s.split(",")[0]), int(s.split(",")[1]), int(s.split(",")[2])) if len(s.split(",")) > 2 else (int(s.split(",")[0]), int(s.split(",")[1]), None))

    [print(i) for i in rating_rdd.take(10)]

    return rating_rdd


def q1(spark_context: SparkContext, data_frame: DataFrame):
    # TODO: Implement Q1 here
    spark = SparkSession(spark_context)

    data_frame.createOrReplaceTempView("ratings_table")
    result = spark.sql("""
    SELECT userId, COUNT(DISTINCT songId) as total_ratings, AVG(rating) as avg_rating
    FROM ratings_table
    WHERE rating IS NOT NULL
    GROUP BY userId
    HAVING total_ratings >= 100 AND avg_rating < 2""")

    print(result.show())  # Delete

    return result


def q2(spark_context: SparkContext, rdd: RDD):
    return


def q3(spark_context: SparkContext, rdd: RDD):
    # TODO: Implement Q3 here

    print("working on q3")

    # Remove None values
    rdd_notnone = rdd.filter(lambda record: record[2] is not None)

    # Map every entry (userId, songId, rating) into (userId, {songId: rating})
    rdd_songList = rdd_notnone.map(lambda record: (record[0], {record[1]: record[2]}))
    [print(i) for i in rdd_songList.take(10)]
    
    # Reduce by key, i.e. merge the dictionaries such that for each user we get a {songId: rating} dictionary
    # And cache it, so we can broadcast to the cart. product
    rdd_ratings_per_user = rdd_songList.reduceByKey(lambda ratings1, ratings2: {**ratings1, **ratings2})
    
    rdd_cart = rdd_ratings_per_user.cartesian(rdd_ratings_per_user).filter(lambda pair: pair[0][0] < pair[1][0])
    [print(i) for i in rdd_ratings_per_user.take(1)]

    def similarity(user_pair):
        # user_pair = ((user1, {songId: rating}), (user2, {songId: rating}))
        # {songId, rating} o {songId, rating} => {songCommon, (rating1, rating2)}
        # foreach songCommmon
        # do the math

        import math

        common = None
        if len(user_pair[0][1].keys()) < len(user_pair[1][1].keys()):
            common = set(user_pair[0][1].keys()).intersection(user_pair[1][1].keys())
        else:
            common = set(user_pair[1][1].keys()).intersection(user_pair[0][1].keys())
        
        ratings_sum = 0
        rating_sum_sq_user1 = 0
        rating_sum_sq_user2 = 0
        for song in common:
            ratings_sum += user_pair[0][1][song] * user_pair[1][1][song]
            rating_sum_sq_user1 += user_pair[0][1][song]**2
            rating_sum_sq_user2 += user_pair[1][1][song]**2

        sim = ratings_sum/(math.sqrt(rating_sum_sq_user1)*math.sqrt(rating_sum_sq_user2))
        return sim
    #similarities[1][1] > 0.95
    rdd_sim = rdd_cart.map(lambda user_pair: (user_pair[0][0], (user_pair[1][0], similarity(user_pair)))).filter(
        lambda similarities: similarities[1][1] > 0.95)
    [print(i) for i in rdd_sim.take(1)]
    
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
    [print(i) for i in rdd_sim_max.take(1)]

    rdd_sim_max_rounded = rdd_sim_max.map(lambda sim: (sim[0], sim[1][0], round(sim[1][1], 2)))
    [print(i) for i in rdd_sim_max_rounded.take(rdd_sim_max_rounded.count())]

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
    print(type(rdd))  # Delete
    print('Here: ' + str(rdd.count()))  # Delete

    #q1(spark_context, data_frame)

    q2(spark_context, rdd)

    q3(spark_context, rdd)

    q4(spark_context, rdd)

    q5(spark_context, rdd)

    spark_context.stop()
