from pyspark.sql import SparkSession
import os

"""
-------------------------------------------------
   Description :	TODO：读取ES的数据
   SourceFile  :	Demo02_ReadFromES
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
spark = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("SparkSQLAppName") \
    .config("spark.sql.shuffle.partitions", 4) \
    .getOrCreate()

# 2.数据输入
# es.resource：es的索引库
# es.nodes：es的节点信息
# es.read.field.include：指定读取es中的哪些字段
input_df = spark.read \
    .format("es") \
    .option("es.resource", "hive_test") \
    .option("es.nodes", "up01:9200") \
    .option("es.read.field.include", "id,name") \
    .load()

# 3.数据处理
input_df = input_df.where("id > 3")

# 4.数据输出
input_df.printSchema()
input_df.show()

# 5.关闭SparkSession
spark.stop()
