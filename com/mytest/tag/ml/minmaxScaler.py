# coding:utf8
import os

from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

"""
-------------------------------------------------
   Description :	TODO：SparkSQL模版
   SourceFile  :	Demo05_MapFunction
   Author      :	81196
   Date	       :	2023/9/7
-------------------------------------------------
"""

# 共享变量--driver中的本地数据和executor中的rdd数据需要一起进行运算时使用
# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['HADOOP_HOME'] = '/export/server/hadoop'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

"""
DataFrame的组成：
1、结构层面：
（1）StrucType对象描述整个DataFrame的表结构
（2）StrucField对象描述一个列的信息
2、数据层面：
（1）Row对象记录一行数据
（2）Column对象记录一列数据并包含列的信息

"""

if __name__ == '__main__':
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
    # Vector是向量(矩阵行)，用于构建sparkml处理的矩阵
    dataFrame = spark.createDataFrame([
        (0, Vectors.dense([1.0, 0.1, -1.0]),),
        (1, Vectors.dense([2.0, 1.1, 1.0]),),
        (2, Vectors.dense([3.0, 10.1, 3.0]),)],
        ["id", "features"])
    # 3.数据处理
    # 对向量进行规划处理
    min_max_scaler = MinMaxScaler().setInputCol("features").setOutputCol("minMaxfeatures")
    min_max_scaler_Model = min_max_scaler.fit(dataFrame)  # x和y映射好形成模型即函数
    resultDF = min_max_scaler_Model.transform(dataFrame)
    # 4.数据输出
    dataFrame.show()
    print('-----------')
    resultDF.show()
    """
       +---+--------------+
        | id|      features|
        +---+--------------+
        |  0|[1.0,0.1,-1.0]|
        |  1| [2.0,1.1,1.0]|
        |  2|[3.0,10.1,3.0]|
        +---+--------------+
    列向量操作
    (x-min)/(max-min)
    +---+--------------+--------------+
    | id|      features|minMaxfeatures|
    +---+--------------+--------------+
    |  0|[1.0,0.1,-1.0]| [0.0,0.0,0.0]|
    |  1| [2.0,1.1,1.0]| [0.5,0.1,0.5]|
    |  2|[3.0,10.1,3.0]| [1.0,1.0,1.0]|
    +---+--------------+--------------+
    """
    # 5.关闭SparkContext
    spark.stop()
