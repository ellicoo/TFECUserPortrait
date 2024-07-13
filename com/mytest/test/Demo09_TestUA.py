import user_agents
from pyspark.sql import SparkSession
import os

"""
-------------------------------------------------
   Description :	TODO：测试UA解析
   SourceFile  :	Demo09_TestUA
   Author      :	mytest team
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'

useragent = "Mozilla/5.0 (X11; NetBSD) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.116 Safari/537.36"

#parse函数解析ua信息，得到一个ua的对象
userAgent = user_agents.parse(useragent)
print("----------------userAgent----------------")
print(userAgent)
print("-----------------os-----------------")
print(userAgent.os.family)
print(userAgent.os.version)
print("-----------------device-----------------")
print(userAgent.device.family)
print(userAgent.device.brand) #品牌/设备（华为品牌/设备）
print("-----------------browser-----------------")
print(userAgent.browser.family)
print(userAgent.browser.version)

