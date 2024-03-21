from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame
from pyspark.broadcast import Broadcast

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

    print(result.show()) #Delete
    
    return result


def q2(spark_context: SparkContext, rdd: RDD):


    rddRatingsAsTuples = rdd.map(lambda s: (int(s.split(",")[0]), int(s.split(",")[1]), float(s.split(",")[2]) if len(s.split(",")) == 3 else -1.0))
        
    rddActualRatingsAsTuples = rddRatingsAsTuples.filter(lambda r: r[2] > 0.0)
        
    rddRatingCounts = rddActualRatingsAsTuples.map(lambda r: (r[0], (1, r[2])))
        
    rddRatingsByUser = rddRatingCounts.reduceByKey(lambda r1, r2: (r1[0] + r2[0], r1[1] + r2[1]))
        
    rddRatingsByUserAtLeast100 = rddRatingsByUser.filter(lambda r: r[1][0] >= 100)
        
    rddAverageRatingByUserAtLeast100 = rddRatingsByUserAtLeast100.map(lambda r1: (r1[0], r1[1][1]/r1[1][0]))
        
    ans = rddAverageRatingByUserAtLeast100.reduce(lambda r1, r2: r1 if r1[1] < r2[1] else r2)

    print('Q2: ' + str(ans))

    return ans


def q3(spark_context: SparkContext, rdd: RDD):
    # TODO: Implement Q3 here
    return


def q4(spark_context: SparkContext, rdd: RDD):
    # TODO: Implement Q4 here
    return

''' First Try
<<<<<<< Updated upstream
def q5(spark_context: SparkContext, rdd: RDD):

    #print(str(rddRatingsAsTuples.count()))
    rddActualRatingsAsTuples = rdd.filter(lambda record: record[2] is not None)
    print("debug2")
    #print(str(rddActualRatingsAsTuples.count()))
    rddRatingCounts = rddActualRatingsAsTuples.map(lambda r: (r[0], 1))
    print("debug3")
    #print(str(rddRatingCounts.count()))
    rddRatingsByUser = rddRatingCounts.reduceByKey(lambda a,b: a+b)
    print("debug4")
    #print(str(rddRatingsByUser.count()))
    #print(rddRatingsByUser.collect())
    rddRatingsByUserAtMost8000 = rddRatingsByUser.filter(lambda r: r[1] <= 8000)
    print("debug5")
    #print(str(rddRatingsByUserAtMost8000.count()))
    #print(rddRatingsByUserAtMost8000.collect())
    rddSorted = rddRatingsByUserAtMost8000.sortBy(lambda x: x[1])
    print("debug6")
    count = int(rddSorted.count())
    #print(count)
    #print(rddSorted.collect())
    print("debug7")
    rddSortedUserIDs = rddSorted.map(lambda x: x[0])
    CollectedUserIDs = rddSortedUserIDs.collect()

    rddSortedValues = rddSorted.map(lambda x: x[1])
    CollectedValues = rddSortedValues.collect()

    matr = []
    arr = []

    print("debug8")
    for i in range(0,count):
        for j in range(i+1,count):
            if CollectedValues[i]*2 + CollectedValues[j] <= 8000:
                for k in range(j+1,count):
                    if CollectedValues[i] + CollectedValues[j] + CollectedValues[k] <= 8000:
                        arr = [CollectedUserIDs[i],CollectedUserIDs[j],CollectedUserIDs[k],CollectedValues[i] + CollectedValues[j] + CollectedValues[k]]
                        matr.append(arr)
                    else:
                        break
            else:
                break
    print("debug9")

    print('Q5: ')
    print(matr)

    return
'''
def q5(spark_context: SparkContext, rdd: RDD):
    # TODO: Implement Q5 here
    
    rddActualRatingsAsTuples = rdd.filter(lambda record: record[2] is not None)

    rddRatingCounts = rddActualRatingsAsTuples.map(lambda r: (r[0], [r[1]]))

    rddRatingsByUser = rddRatingCounts.reduceByKey(lambda a,b: a+b)

    rddRatingsByUserAtMost8000 = rddRatingsByUser.filter(lambda r: len(r[1]) <= 8000)

    sorted_rdd = rddRatingsByUserAtMost8000.sortBy(lambda x: x[0])
   
    cartesian_rdd = sorted_rdd.cartesian(sorted_rdd)

    nonsame_rdd = cartesian_rdd.filter(lambda x: x[0][0] < x[1][0])
    print("debug0")

    merged_rdd = nonsame_rdd.map(lambda x: ((x[0][0], x[1][0]), (x[0][1] + x[1][1])))
    print("debug1")
    filtered_rdd = merged_rdd.mapValues(lambda x: list(set(x)))

    #print(filtered_rdd.take(2))
    print("debug2")
    rddRatingsByTwoUsersAtMost8000 = filtered_rdd.filter(lambda r: len(r[1]) <= 8000)
    print("debug2.5")
    print(rddRatingsByTwoUsersAtMost8000.take(1))
    
    print("debug3")
    second_cartesian_rdd = rddRatingsByTwoUsersAtMost8000.cartesian(rddRatingsByUserAtMost8000)
    print("debug4")
    second_nonsame_rdd = second_cartesian_rdd.filter(lambda x: x[0][0] != x[1][0]).filter(lambda x: x[0][1] != x[1][0])
    print("debug5")
    second_merged_rdd = second_nonsame_rdd.map(lambda x: ((x[0][0][0], x[0][0][1], x[1][0]), (x[0][1] + x[1][1])))
    print("debug6")
    second_filtered_rdd = second_merged_rdd.mapValues(lambda x: list(set(x)))
    print("debug7")
    rddRatingsByThreeUsersAtMost8000 = second_filtered_rdd.filter(lambda r: len(r[1]) <= 8000)
    print("debug8")
    result = rddRatingsByThreeUsersAtMost8000.map(lambda x: ((x[0][0], x[0][1], x[0][2], len(x[1]))))
    #print(result.collect())
=======
def q5(spark_context: SparkContext, rdd: RDD):

    #print(str(rddRatingsAsTuples.count()))
    rddActualRatingsAsTuples = rdd.filter(lambda record: record[2] is not None)
    print("debug2")
    #print(str(rddActualRatingsAsTuples.count()))
    rddRatingCounts = rddActualRatingsAsTuples.map(lambda r: (r[0], 1))
    print("debug3")
    #print(str(rddRatingCounts.count()))
    rddRatingsByUser = rddRatingCounts.reduceByKey(lambda a,b: a+b)
    print("debug4")
    #print(str(rddRatingsByUser.count()))
    #print(rddRatingsByUser.collect())
    rddRatingsByUserAtMost8000 = rddRatingsByUser.filter(lambda r: r[1] <= 8000)
    print("debug5")
    #print(str(rddRatingsByUserAtMost8000.count()))
    #print(rddRatingsByUserAtMost8000.collect())
    rddSorted = rddRatingsByUserAtMost8000.sortBy(lambda x: x[1])
    print("debug6")
    count = int(rddSorted.count())
    #print(count)
    #print(rddSorted.collect())
    print("debug7")
    rddSortedUserIDs = rddSorted.map(lambda x: x[0])
    CollectedUserIDs = rddSortedUserIDs.collect()

    rddSortedValues = rddSorted.map(lambda x: x[1])
    CollectedValues = rddSortedValues.collect()

    matr = []
    arr = []

    print("debug8")
    for i in range(0,count):
        for j in range(i+1,count):
            if CollectedValues[i]*2 + CollectedValues[j] <= 8000:
                for k in range(j+1,count):
                    if CollectedValues[i] + CollectedValues[j] + CollectedValues[k] <= 8000:
                        arr = [CollectedUserIDs[i],CollectedUserIDs[j],CollectedUserIDs[k],CollectedValues[i] + CollectedValues[j] + CollectedValues[k]]
                        matr.append(arr)
                    else:
                        break
            else:
                break
    print("debug9")

    print('Q5: ')
    print(matr)
>>>>>>> Stashed changes

    return
'''
def q5(spark_context: SparkContext, rdd: RDD):

    # Filter out records with non-null ratings
    rddActualRatingsAsTuples = rdd.filter(lambda record: record[2] is not None)

    # Map each user to their ratings
    rddRatingCounts = rddActualRatingsAsTuples.map(lambda r: (r[0], [r[1]]))

    # Reduce by key to combine ratings for each user
    rddRatingsByUser = rddRatingCounts.reduceByKey(lambda a, b: a + b)

    # Filter users with at most 8000 ratings
    rddRatingsByUserAtMost8000 = rddRatingsByUser.filter(lambda r: len(r[1]) <= 8000)
    # Sort users
    sorted_user_rdd = rddRatingsByUserAtMost8000.sortBy(lambda x: x[0]).map(lambda x: x[0])

    # Compute Cartesian product of sorted_user_rdd
    # (user1) (user2)
    cartesian_rdd = sorted_user_rdd.cartesian(sorted_user_rdd).filter(lambda x: x[0] < x[1])
    # ((user1, user2))(user3)
    double_cartesian_rdd = cartesian_rdd.cartesian(sorted_user_rdd).filter(lambda x: x[0][1] < x[1])
    print("ananin ami")
    # Map to calculate total RDD
    #confuse_rdd = double_cartesian_rdd.map(lambda x:((x[0][0], (x[0][0], x[0][1], x[1])), (x[0][1], (x[0][0], x[0][1], x[1])), (x[1], (x[0][0], x[0][1], x[1]))))#.join(rddRatingsByUserAtMost8000)
    confuse_rdd = double_cartesian_rdd.map(lambda x:(x[0][0], (x[0][1], x[1])))#.join(rddRatingsByUserAtMost8000)
    print(confuse_rdd.take(2))
    rdd2 = spark_context.parallelize([(0, (1, 2)), (0, (1, 3))])
    confuse_rdd_join = rdd2.join(rddRatingsByUserAtMost8000)
    print(confuse_rdd_join.collect())
    confuse1_rdd = confuse_rdd.map(lambda x:(x[1][0], (x[0], x[1][1], x[1][2]))).join(rddRatingsByUserAtMost8000)
    confuse2_rdd = confuse1_rdd.map(lambda x:(x[1][1], (x[0], x[0][1], x[1][2]+x[1][3]))).join(rddRatingsByUserAtMost8000)
    print(confuse2_rdd.take(2))
    total_rdd = confuse2_rdd.map(lambda x: ((x[0], x[1][0], x[1][1]), list(set(x[1][2])+set(x[1][3]))))
    result = total_rdd.filter(lambda r: len(r[1]) <= 8000)
    print(result.collect())

    '''
    flattened_rdd = confuse_rdd.flatMap(lambda x: x).map(lambda x: (x[0], x[1]))
    print(flattened_rdd.take(2))
    print("erenin anasinin ami")
    total_rdd = confuse_rdd.map(lambda x: (x[1][0], x[1][1])).reduceByKey(lambda x, y: x + y)
    print(total_rdd.take(2))
    # Map values to remove duplicates
    distinct_rdd = total_rdd.mapValues(lambda x: list(set(x)))

    # Filter result with at most 8000 ratings
    result = distinct_rdd.filter(lambda r: len(r[1]) <= 8000)

    print(result.collect())
    '''
    return result


if __name__ == '__main__':
    on_server = False  # TODO: Set this to true if and only if deploying to the server

    spark_context = get_spark_context(on_server)

    #data_frame = q0a(spark_context, on_server)
    #print(type(data_frame)) #Delete
    #print(data_frame.show()) #Delete

    rdd = q0b(spark_context, on_server)
    #print(type(rdd)) #Delete
    #print('Here: ' + str(rdd.count())) #Delete

    #q1(spark_context, data_frame)

    #q2(spark_context, rdd)

    q4(spark_context, rdd)

    q5(spark_context, rdd)

    spark_context.stop()