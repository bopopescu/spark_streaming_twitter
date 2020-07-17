import json
import os

import mysql.connector as m
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64/'

conf = SparkConf()
conf.setAppName("twitter_spark111111")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 2)
ssc.checkpoint("check_twitter_app")
dataStream = ssc.socketTextStream("localhost", 3000)


def insert_into_tweets(list_rdd):
    mydb = m.connect(
        host="localhost",
        user="root",
        database='SparkDB',
        autocommit=True,
        allow_local_infile=True
    )
    mycursor = mydb.cursor()
    mycursor.executemany("""
        INSERT INTO tweets (tweet_id,hashtag,place,date)
        VALUES (%(id)s, %(hashtag)s, %(place)s, %(created_at)s)""", list_rdd)
    mydb.commit()


def is_proper_string(s):
    try:
        s.encode(encoding='utf-8').decode('ascii')
    except UnicodeDecodeError:
        return False
    else:
        return True


def get_tweets_from_dictionary(time, rdd):
    dictionary_rdd = rdd.map(lambda x: json.loads(x))
    rdd_list = dictionary_rdd.collect()
    updated_list = []
    for i in rdd_list:
        if len(i['hashtags']) > 0:
            for j in i['hashtags']:
                if len(j['text']) < 150 and is_proper_string(j['text']) and is_proper_string(i['place']):
                    d = {'id': i['id'], 'hashtag': j['text'], 'place': i['place'], 'created_at': i['created_at']}
                    updated_list.append(d)

    insert_into_tweets(updated_list)


dataStream.foreachRDD(get_tweets_from_dictionary)

ssc.start()
ssc.awaitTermination()
