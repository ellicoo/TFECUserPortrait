# coding:utf8
import os

from pyspark.ml.feature import StringIndexer, IndexToString
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

# import pandas as pd

"""
-------------------------------------------------
   Description :	TODO：特征工程
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
    # 特征工程--将非数值型转数值型
    df = spark.createDataFrame([(0, 'a'), (1, 'b'), (2, 'c'), (3, 'a'), (5, 'c')],
                               ["id", "category"])
    # 3.数据处理
    # setInputCol --输入列：需要从df中获取
    # setOutputCol--输出列：用户自定义的列
    string_indexer = StringIndexer().setInputCol("category").setOutputCol("categoryIndex")
    resultDF = string_indexer.fit(df).transform(df)

    # 4.数据输出
    df.printSchema()
    df.show()
    print('-------------------')
    resultDF.printSchema()
    resultDF.show()
    '''
    +---+--------+-------------+
    | id|category|categoryIndex|
    +---+--------+-------------+
    |  0|       a|          0.0|
    |  1|       b|          2.0|
    |  2|       c|          1.0|
    |  3|       a|          0.0|
    |  5|       c|          1.0|
    +---+--------+-------------+
    '''
    print('---------------------')
    indexer = \
        IndexToString(). \
            setInputCol("categoryIndex"). \
            setOutputCol("cateLable")
    # setLabels(string_indexer.lable) 不需要给参数
    indexer.transform(resultDF).show()
    # 5.关闭SparkContext
    spark.stop()
