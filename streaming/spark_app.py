
import sys
import requests
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
import json
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from datetime import datetime
import re
from collections import Counter
import functools

DATA_SOURCE_IP = "data-source"
DATA_SOURCE_PORT = 9999
sc = SparkContext(appName="GitHubAPI")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 60)
ssc.checkpoint("checkpoint_GitHubAPI")
data = ssc.socketTextStream(DATA_SOURCE_IP, DATA_SOURCE_PORT)


def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def time_checker(rdd_time):
    now = datetime.now()
    now = now.strftime("%Y%m%d%H%M%S")
    now = now.replace(':', '')

    rdd_time = rdd_time.replace('-', '')
    rdd_time = rdd_time.replace(':', '')
    rdd_time = rdd_time.replace('T', '')
    rdd_time = rdd_time.replace('Z', '')
    if (int(now) - int(rdd_time) <= 10000):
        return "True"
    return "False"

def splitter(x):
    x = re.sub('[^a-zA-Z ]', '', x)
    x = x.lower().split(' ')
    return x

def counter_func(x):
    x = list(filter(lambda a: a != 'none', x))
    x = list(filter(lambda a: a != '', x))
    filtered_list = Counter(x)
    occurences = filtered_list.most_common(10)
    return occurences

def aggregate_words_count(new_list, total_list):
    if len(new_list) != 0:
        oldList = new_list[0]
        updatedList = []
        if total_list:
            totalListDict = dict(total_list)
            for new_words in oldList:
                if new_words[0] in totalListDict.keys():
                    totalListDict[new_words[0]] = totalListDict[new_words[0]] + new_words[1]
                else:
                    totalListDict[new_words[0]] = new_words[1]
            dictList = [(k, v) for k, v in totalListDict.items()]
            updatedList = sorted(dictList, key=lambda tup: tup[1], reverse = True)
            return updatedList[:10]
        else:
            newList = sorted(oldList, key = lambda x: x[1], reverse = True)
            updatedList = newList
            return updatedList[:10]
    else:
        return new_list

def send_df_to_dashboard(df):
    url = 'http://webapp:5000/updateData'
    data = df.toPandas().to_dict('list')
    requests.post(url, json=data)

def send_df_to_dashboard2(df):
    url = 'http://webapp:5000/updateData2'
    data = df.toPandas().to_dict('list')
    requests.post(url, json=data)

def send_df_to_dashboard3(df):
    url = 'http://webapp:5000/updateData3'
    data = df.toPandas().to_dict('list')
    requests.post(url, json=data)

def send_df_to_dashboard4(df):
    url = 'http://webapp:5000/updateData4'
    data = df.toPandas().to_dict('list')
    requests.post(url, json=data)

def get_sql_context_instance(spark_context):
    if('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SparkSession(spark_context)
    return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
    pass
    print("----------- %s -----------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(LanguageName=w[0], Count=w[1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results")
        new_results_df = sql_context.sql("select LanguageName, Count from results order by Count DESC LIMIT 3")
        new_results_df.show()
        send_df_to_dashboard(new_results_df)
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

def process_rdd2(time, rdd):
    pass
    print("----------- %s -----------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(LanguageName=w[0], Changes=w[1], time=w[2]  ))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results")
        new_results_df = sql_context.sql("select LanguageName, Changes, time from results order by Changes DESC LIMIT 3")
        new_results_df.show()
        #send_df_to_dashboard(new_results_df)
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

def process_rdd3(time, rdd):
    pass
    print("----------- %s -----------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(LanguageName=w[0], AverageStars=w[1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results")
        new_results_df = sql_context.sql("select LanguageName, AverageStars from results order by AverageStars DESC LIMIT 3")
        new_results_df.show()
        send_df_to_dashboard3(new_results_df)
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

def process_rdd4(time, rdd):
    pass
    print("----------- %s -----------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(LanguageName=w[0], TopWords=w[1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results")
        new_results_df = sql_context.sql("select LanguageName, TopWords from results order by TopWords DESC LIMIT 3")
        new_results_df.show()
        send_df_to_dashboard4(new_results_df)
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)
words = data.map(lambda line: line.split("\t"))

duplicates = words.map(lambda x: (x[0], x[1], x[3], x[4]))
duplicates = duplicates.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)

lang_count = duplicates.map(lambda word: (word[0][0], 1)).reduceByKey(lambda a, b: a + b)
lang_count = lang_count.updateStateByKey(aggregate_tags_count)
lang_count.pprint()
lang_count.foreachRDD(process_rdd) 

#batch_count = words.map(lambda word: (word[0], word[2], 1))
#batch_count = batch_count.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
#batch_count = batch_count.updateStateByKey(aggregate_tags_count)
#batch_count.pprint()
#batch_count.foreachRDD(process_rdd2)


#new_words = words.map(lambda x: (x[0], x[1], time_checker(x[2]), x[3], x[4]))
#new_words = new_words.map(lambda word: (word[0], 1)) if new_words.map(lambda x: (x[2])) == "True" else new_words.map(lambda word: (word[0], 0))
#new_words.pprint()
#new_words = new_words.reduceByKey(lambda a, b: a + b)
#new_words.pprint()
#batch_count = new_words.map(lambda word: (word[0],1)) if new_words.map(lambda x: (x[2])) == True else new_words.map(lambda word: (word[0], 0))
#batch_count = batch_count.reduceByKey(lambda a, b: a + b)
#batch_count = batch_count.updateStateByKey(aggregate_tags_count)
#batch_count.pprint()
#batch_count.foreachRDD(process_rdd2) 

stars_count = duplicates.map(lambda word: (word[0][0], int(word[0][2]))).reduceByKey(lambda a, b: a + b)
stars_count = stars_count.updateStateByKey(aggregate_tags_count)
stars_zip = stars_count.map(lambda x: (x[0], x[1]))
lang_zip = lang_count.map(lambda x: (x[0], x[1]))
rdd_join = stars_zip.join(lang_zip).mapValues(lambda x: x[0] / x[1] if(x[1]> 0) else 0)
rdd_join = rdd_join.map(lambda x: (x[0], round(x[1], 1)))
rdd_join.pprint()
rdd_join.foreachRDD(process_rdd3) 


count = duplicates.map(lambda word: (word[0][0], splitter(word[0][3]))).reduceByKey(lambda a, b: a + b)
count = count.map(lambda x: (x[0], counter_func(x[1])))
count = count.updateStateByKey(aggregate_words_count)

count.pprint()
count.foreachRDD(process_rdd4)

ssc.start()
ssc.awaitTermination()
