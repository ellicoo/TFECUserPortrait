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

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
import tensorflow as tf

# 创建 Spark 会话
spark = SparkSession.builder.appName("TensorFlow with Spark").getOrCreate()

# 加载模型
model = tf.keras.models.load_model('/tmp/tf_model')

# 定义 UDF 函数来进行预测
def predict(input_value):
    prediction = model.predict([[input_value]])
    return float(prediction[0][0])

# 注册 UDF
predict_udf = udf(predict, FloatType())

# 创建 Spark DataFrame
data = [(1.0,), (2.0,), (3.0,), (4.0,)]
df = spark.createDataFrame(data, ["input"])

# 使用 UDF 进行预测
df = df.withColumn("prediction", predict_udf(df["input"]))
df.show()
