from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as F

"""
-------------------------------------------------
   Description :	TODO：测试解析Json的函数
   SourceFile  :	Demo10_TestGetJsonObject
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

# 如果数据只有一列，需要使用,隔开（特殊）
input_df = spark.createDataFrame(
    data=[('{"user_id":"abcd","name":"zhangsan","properties":{"age":"18","address":"宝安"}}',)],
    schema=['value'])

# get_json_object
# {"user_id":"abcd","name":"zhangsan","properties":{"age":"18","address":"宝安"}}
# get_json_object：可以解析嵌套json，但是只能一次性解析一个数据
json_object = input_df.select(F.get_json_object("value", "$.user_id").alias("user_id"),
                              F.get_json_object("value", "$.name").alias("name"),
                              F.get_json_object("value", "$.properties").alias("properties"),
                              F.get_json_object("value", "$.properties.age").alias("age"),
                              F.get_json_object("value", "$properties.address").alias("address"))
json_object.printSchema()
json_object.show()

# get_tuple
# get_tuple：不能解析嵌套json，可以一次性解析多个数据
json_tuple = input_df.select(
    F.json_tuple("value", "user_id", "name", "properties").alias("user_id", "name", "properties"))

json_tuple.printSchema()
json_tuple.show()
input_rdd = input_df.rdd
input_rdd.reduceByKey()
input_rdd.foldByKey()
input_rdd.aggregateByKey()

# 关闭SparkSession
spark.stop()
