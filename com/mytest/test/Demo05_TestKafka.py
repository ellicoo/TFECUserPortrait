from pyspark.sql import SparkSession
import os

"""
-------------------------------------------------
   Description :	TODO：测试读写Kafka
   SourceFile  :	Demo05_TestKafka
   Author      :	mytest team
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'

# 构建SparkSession
# 建造者模式：类名.builder.配置…….getOrCreate()
# 自动帮你构建一个SparkSession对象，只要指定你需要哪些配置就可
spark = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("SparkSQLAppName") \
    .config("spark.sql.shuffle.partitions", 4) \
    .getOrCreate()

print("-------------------1.从Kafka中读取数据---------------------")
# 默认情况下，结构化流只会消费最新（最近） 的数据，不会消费历史数据。
# 这是有一个参数决定的：startingOffsets，流式任务中默认从最近的数据开始消费。可以设置从起始的位置开始消费。
# startingOffsets：earliest（最早的）、latest（最近的）
input_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "up01:9092") \
    .option("startingOffsets", "earliest") \
    .option("subscribe", "test_flume_kafka_sink") \
    .load()

input_df.printSchema()
# show会报错：流式任务还没启动
# input_df.show()
# select：只能写DSL风格
# selectExpr：可以写SQL风格
input_df = input_df.selectExpr("key", "value", "partition")

# Sink到控制台
query1 = input_df.writeStream.format("console").outputMode("append")

print("-------------------2.把数据写入到Kafka---------------------")

# Sink到Kafka
# checkpointLocation：输出到文件或者kafka时，需要指定Checkpoint的目录
query2 = input_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "up01:9092") \
    .option("topic", "test_kafka_topic_write") \
    .option("checkpointLocation", "/root/ckp") \
    .outputMode("append")

# 启动流式任务
# 可以启动多个Sink端，但是只能允许最后一个流式任务阻塞执行（awaitTermination()）
query1.start()
query2.start().awaitTermination()
