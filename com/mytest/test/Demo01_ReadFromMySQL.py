from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F

"""
-------------------------------------------------
   Description :	TODO：读取MySQL的数据
   SourceFile  :	Demo01_ReadFromMySQL
   Author      :	81196
   Date	       :	2023/9/25
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'

# 1.构建SparkSession
# 建造者模式：类名.builder.配置…….getOrCreate()
# 自动帮你构建一个SparkSession对象，只要指定你需要哪些配置就可
# spark对象的master方法--表示 Spark 应用程序将在本地运行，并使用 2 个线程。通常在开发和测试环境中使用这种设置，以便在本地计算机上并行执行任务。
#
spark = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("SparkSQLAppName") \
    .config("spark.sql.shuffle.partitions", 4) \
    .getOrCreate()

# 2.数据输入
input_df = spark.read.jdbc(url='jdbc:mysql://up01:3306/edu',
                           table='base_region',
                           properties={"user": "root", "password": "123456"})

# 3.数据处理--我习惯用filter
# input_df = input_df.where("id > 3")
input_df = input_df.filter(F.col("id") > 3)

# 4.数据输出
input_df.printSchema()
input_df.show()
# url:实际工作中，不需要自己写，直接拿一个过来用即可。
# jdbc:没有save
# format：有save
# mode:模式，append、overwrite、ignore、error（默认）
input_df.write \
    .jdbc(url='jdbc:mysql://up01:3306/edu?useUnicode=true&characterEncoding=UTF-8&useSSL=false',
          table='base_region2',
          mode='overwrite',
          properties={"user": "root", "password": "123456"})

# 5.关闭SparkSession
spark.stop()
