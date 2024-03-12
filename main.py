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

    return spark_context.textFile(plays_file_path)


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

    print(result.show()) #Delete
    
    return result


def q2(spark_context: SparkContext, rdd: RDD):
    return


def q3(spark_context: SparkContext, rdd: RDD):
    # Initialize Spark Context
    conf = SparkConf().setAppName("User Similarity")
    sc = SparkContext(conf=conf)

    # Function to compute cosine similarity
    def cosine_similarity(vector1, vector2):
        dot_product = sum(a * b for a, b in zip(vector1, vector2))
        norm_a = math.sqrt(sum(a * a for a in vector1))
        norm_b = math.sqrt(sum(b * b for b in vector2))
        return dot_product / (norm_a * norm_b)

    # Load and preprocess data
    data = sc.textFile("plays.csv").map(lambda line: line.split(","))

    # Map data to ((user, song), rating) and reduce to handle any duplicates
    user_song_ratings = data.map(lambda x: ((int(x[0]), int(x[1])), float(x[2]))).reduceByKey(lambda x, y: x + y)

    # Map to (user, (song, rating)) and group by user
    user_ratings_group = user_song_ratings.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey()

    # Convert group data into user and SparseVector format
    user_ratings_vector = user_ratings_group.map(lambda x: (x[0], SparseVector(10000, {i[0]: i[1] for i in list(x[1])}))) # Assuming a max of 10k songs

    # Self join to compute similarities with filter to ensure A < B and avoid duplicate calculations
    similarities = user_ratings_vector.cartesian(user_ratings_vector) \
        .filter(lambda x: x[0][0] < x[1][0]) \
        .map(lambda x: ((x[0][0], x[1][0]), cosine_similarity(x[0][1].toArray(), x[1][1].toArray()))) \
        .filter(lambda x: x[1] > 0.95)

    # Find the best match for each user under the specified conditions
    best_matches = similarities.reduceByKey(lambda a, b: a if a[1] > b[1] else b) \
        .map(lambda x: f">> Q3: <{x[0][0]}, {x[0][1]}, {round(x[1], 2)}>").collect()

    # Print the results
    for match in best_matches:
        return match

    # Stop the Spark context
    sc.stop()


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
    print(type(data_frame)) #Delete
    print(data_frame.show()) #Delete

    rdd = q0b(spark_context, on_server)
    print(type(rdd)) #Delete
    print('Here: ' + str(rdd.count())) #Delete

    q1(spark_context, data_frame)

    q2(spark_context, rdd)

    q4(spark_context, rdd)

    q5(spark_context, rdd)

    spark_context.stop()
