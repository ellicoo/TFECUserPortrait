import datetime

from pyspark.sql import SparkSession
import os
from datetime import datetime

"""
-------------------------------------------------
   Description :	TODO：测试日期格式转换
   SourceFile  :	Demo07_TestDateTime
   Author      :	mytest team
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'

# 给定的时间格式
dateStr = '13/Oct/2022:19:46:28 +0800'
# replace("old_str","new_str")：把旧字符串( +0800)替换为新字符串("")

dateStr = dateStr.replace(" +0800", "")

print(dateStr)

# 中间结果对象
dateObj = datetime.strptime(dateStr, '%d/%b/%Y:%H:%M:%S')

# 目标格式：年-月-日 时:分:秒
res = dateObj.strftime('%Y-%m-%d %H:%M:%S')

# 输出
print(res)
