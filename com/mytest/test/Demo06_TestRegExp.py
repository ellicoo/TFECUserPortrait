from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as F

"""
-------------------------------------------------
   Description :	TODO：
   SourceFile  :	Demo06_TestRegExp
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

log = '33.169.220.221 - - [13/Oct/2022:19:46:28 +0800] "GET /js/20.b9c086d.js HTTP/1.1" 200 159997 "-" "Mozilla/5.0 (X11; NetBSD) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.116 Safari/537.36" "-"'

"""
Java版本:
(?<ip>\d+\.\d+\.\d+\.\d+) (- - \[)(?<datetime>[\s\S]+)(?<t1>\][\s"]+)(?<request>[A-Z]+) (?<url>[\S]*) (?<protocol>[\S]+)["] (?<code>\d+) (?<sendbytes>\d+) ["](?<refferer>[\S]*) ["](?<useragent>[\S\s]+)["] ["](?<proxyaddr>[\S\s]+)["]

"""

regexp = '(?<ip>\d+\.\d+\.\d+\.\d+) (- - \[)(?<datetime>[\s\S]+)(?<t1>\][\s"]+)(?<request>[A-Z]+) (?<url>[\S]*) (?<protocol>[\S]+)["] (?<code>\d+) (?<sendbytes>\d+) ["](?<refferer>[\S]*) ["](?<useragent>[\S\s]+)["] ["](?<proxyaddr>[\S\s]+)["]'

ip = F.regexp_extract(log, regexp, 1)
# ip = F.regexp_extract(log, regexp)

print(ip)

# 关闭SparkSession
spark.stop()
