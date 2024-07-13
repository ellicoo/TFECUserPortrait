from pyspark.sql import SparkSession
import os

"""
-------------------------------------------------
   Description :	TODO：写数据到ES中
   SourceFile  :	Demo03_WriteToES
   Author      :	81196
   Date	       :	2023/9/25
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python3'

# 1.构建SparkSession
# 建造者模式：类名.builder.配置…….getOrCreate()
# 自动帮你构建一个SparkSession对象，只要指定你需要哪些配置就可
# thrift://up01:9083 表示 Hive Metastore 服务器的地址和端口
spark = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("SparkSQLAppName") \
    .config("spark.sql.shuffle.partitions", 4) \
    .config("hive.metastore.uris", "thrift://up01:9083") \
    .config("spark.sql.warehouse.dir", "hdfs://up01:8020/user/hive/test/hdfs") \
    .enableHiveSupport() \
    .getOrCreate()

# 2.数据输入
# table:读取Hive表数据
input_df = spark.read.table("default.tb_hdfs")

# 3.数据处理
input_df = input_df.where("id > 4")

# 4.数据输出
input_df.printSchema()
input_df.show()
# es.mapping.id：设置es中的id，如果不设置，则会自动生成一个id，最好要设置上
input_df.write \
    .format("es") \
    .mode("append") \
    .option("es.resource", "hive_test") \
    .option("es.nodes", "up01:9200") \
    .option("es.mapping.id", "id") \
    .save()

# 5.关闭SparkSession
spark.stop()
