
# get average number of tweets per day from each user
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, unix_timestamp, avg, count, min as spark_min, max as spark_max
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

# Assuming 'non_rt_tweets' is your DataFrame
df = non_rt_tweets

# Convert the 'tweetcreatedts' column to a timestamp type
df = df.withColumn("date", to_date(col("tweetcreatedts")))

# Create window partitioned by user and ordered by date
window = Window.partitionBy("username").orderBy("date")

# Calculate the number of tweets per day for each user
df = df.withColumn("daily_tweet_count", count("text").over(window))

# Calculate the average number of tweets per day for each user
result_df = df.groupBy("username").agg(
    avg("daily_tweet_count").alias("avg_tweets_per_day")
)

# Display the result
result_df.show()

# Calculate the time difference
time_difference = (
    df.groupBy("username")
    .agg(
        (unix_timestamp(spark_max("date")) - unix_timestamp(spark_min("date"))).cast(LongType()).alias("time_active_seconds")
    )
)

# Display the result
time_difference.show()

# If you want to convert the result to a Pandas DataFrame
time_difference_df = time_difference.toPandas()
print(time_difference_df)
username_counts = df.groupBy("username").count()

# Display the result
username_counts.show()
copy_df = df.withColumn("date", F.unix_timestamp(col("date")))
# If you want to convert the result to a Pandas DataFrame
username_counts_df = username_counts.toPandas()
'''
from pyspark.sql.functions import col, to_date
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import pandas as pd
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.types import LongType

# make copy of dataframe and convert each tweet created time stamp into a time stamp object
df = non_rt_tweets
df = df.withColumn("date", to_date(col("tweetcreatedts")))

# create window partitioned by user and ordered by date
window = Window.partitionBy("username").orderBy("date")

# Calculate the number of tweets per day for each user
df = df.withColumn("daily_tweet_count", F.count("text").over(window))
print(df.columns) 
# Calculate the average number of tweets per day for each user
result_df = df.groupBy("username").agg(
    F.avg("daily_tweet_count").alias("avg_tweets_per_day")
)
#df.printSchema();
#result_df.show()
result_df.sample(0.5).show()

def calculate_time_difference(min_date, max_date):
     return (max_date - min_date).total_seconds()

# Register the function as a UDF
#calculate_time_difference_udf = udf(calculate_time_difference, LongType())

# Use groupBy, min, and max to calculate the time difference
time_difference = (
    copy_df.groupBy("username")
    .agg(
        calculate_time_difference(
            F.min("date").cast("timestamp"), F.max("date").cast("timestamp")
        ).alias("time_active_seconds")
    )
)

# Display the result
time_difference.show()

# If you want to convert the result to a Pandas DataFrame
time_difference_df = time_difference.toPandas()

# Display the result as a Pandas DataFrame
print(time_difference_df)'''