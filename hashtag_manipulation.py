import os

from pyspark import SparkContext, SQLContext
from pyspark.sql import functions, Window
from pyspark.sql.functions import date_format, to_date, unix_timestamp, to_timestamp

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars file:<path> pyspark-shell'
sc = SparkContext(appName="TestPySparkJDBC", master="local")
sqlContext = SQLContext(sc)
df = sqlContext.read.format('jdbc').options(driver='com.mysql.jdbc.Driver', url='jdbc:mysql://localhost:3306/<dbname>',
                                            dbtable='tweets').option("user", <username>).option("password", <password>).load()


def get_most_popular_hashtag():
    count_hashtags = df.groupBy('place', 'hashtag').agg(functions.count('hashtag').alias('hashtag_count'))
    most_popular_hashtag = count_hashtags.groupBy('place').agg(functions.max('hashtag_count').alias('max'))

    count_hashtags.join(most_popular_hashtag, ((count_hashtags.hashtag_count == most_popular_hashtag.max) &
                                               (count_hashtags.place == most_popular_hashtag.place))) \
        .select(count_hashtags.place, count_hashtags.hashtag).orderBy('max', ascending=False).show(10)


def get_most_popular_hashtag_by_time():
    df.withColumn("date",
                  to_timestamp(unix_timestamp('date', "EEE MMM dd HH:mm:ss +0000 yyyy").cast("timestamp"))).withColumn(
        'time', date_format('date', "HH:mm:ss"))

    count_hashtags = df.groupBy('place', 'hashtag').agg(functions.count('hashtag').alias('hashtag_count'))
    most_popular_hashtag = count_hashtags.groupBy('place').agg(functions.max('hashtag_count').alias('max'))

    count_hashtags.join(most_popular_hashtag, ((count_hashtags.hashtag_count == most_popular_hashtag.max) &
                                               (count_hashtags.place == most_popular_hashtag.place))) \
        .select(count_hashtags.place, count_hashtags.hashtag).orderBy('max', ascending=False).show(10)

    w = Window.partitionBy("place", "hashtag", "date", "hour")
    per_hour_frequency = most_popular_hashtag.withColumn("date", to_date("created_at")) \
        .withColumn("tag_count", f.count('id').over(w)). \
        select('place', 'date', 'hour', 'hashtag', 'tag_count'). \
        distinct(). \
        sort(functions.asc('place'), functions.asc('hashtag'), functions.asc('date'), functions.asc('hour'),
             functions.asc('tag_count'))


get_most_popular_hashtag_by_time()
get_most_popular_hashtag()
