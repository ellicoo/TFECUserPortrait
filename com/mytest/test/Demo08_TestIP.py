import requests
from pyspark.sql import SparkSession
import os

"""
-------------------------------------------------
   Description :	TODO：IP地址解析
   SourceFile  :	Demo08_TestIP
   Author      :	mytest team
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'

ipStr = "221.169.132.33"

url = f"http://opendata.baidu.com/api.php?query={ipStr}&co=&resource_id=6006&oe=utf8"

# 如何使用代码请求接口呢？
# 这个接口是百度免费提供给用户使用，但撒不要频繁访问
# 这种接口在公司中一般会用付费的，会更稳定

# request()：向URL地址发送请求
# method：请求方式，一般是增删改查（PUT、DELETE、POST、GET）
res = requests.request("GET", url).json()
print(res)
print(res['data'][0]['location'])

# JSON：本质上就是一个字符串，只是这个字符串有一定的格式。
# JSON的格式：Key和Value
# Key:字符串类型
# Value：{}：字典类型
# Value：[]：列表类型
