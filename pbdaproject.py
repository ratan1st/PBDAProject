import sys
import pyspark
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession


def updateTotalCount(currentCount, countState):
    if countState is None:
       countState = 0
    return sum(currentCount, countState)

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']


# Convert RDDs of the words DStream to DataFrame and run SQL query
def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        sqlContext = getSqlContextInstance(rdd.context)

        # Convert RDD[String] to RDD[Row] to DataFrame
        #rowRdd = rdd.map(lambda w: Row(word=w))
        rowRdd = rdd.map(lambda w: Row(word=w[0], cnt=w[1]))
        #rowRdd.pprint()
        wordsDataFrame = sqlContext.createDataFrame(rowRdd)
        wordsDataFrame.show()

        # Creates a temporary view using the DataFrame.
        wordsDataFrame.createOrReplaceTempView("words")

        # Do word count on table using SQL and print it
        wordCountsDataFrame = \
             sc.sql("select SUM(cnt) as total from words")
        wordCountsDataFrame.show()
    except:
       pass

with pyspark.SparkContext("local", "PySparkWordCount") as sc:

    spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

    ssc = StreamingContext(sc, 15)
    ssc.checkpoint("/tmp")
    brokers = 'localhost:9092'
    topic = 'test'
    
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    lines = kvs.map(lambda x: x[1])
    
    counts = lines.filter(lambda line:'GET' in line)\
             .map(lambda line: line.split(' ')[0])\
             .map(lambda line: (line, 1)) \
             .reduceByKey(lambda a, b: a + b)

    totalCounts = counts.updateStateByKey(updateTotalCount)

    # print('-\n-\n-\n-\n-\n-\n-\n-\n-\n')
    # # totalCounts.foreachRDD(lambda rdd: sc.createDataFrame(rdd))
    # totalCounts.foreachRDD(process)

    # print('-\n-\n-\n-\n-\n-\n-\n-\n-\n')

    # people = rdd.map(lambda x: Row(name=x[0], age=int(x[1])))
    # schemaPeople = sqlContext.createDataFrame(people)

    x = totalCounts.pprint()    
    
    ssc.start()
    ssc.awaitTermination()