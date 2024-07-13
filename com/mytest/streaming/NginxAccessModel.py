from datetime import datetime

import requests
import user_agents
from pyspark.sql import SparkSession, DataFrame
import os
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

"""
-------------------------------------------------
   Description :	TODO：结构化流消费Kafka的数据
   SourceFile  :	NginxAccessModel
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

# up01:8020连接失败的问题。
# 产生原因：没有启动HDFS，Spark是配置的是Yarn模式，Yarn模式中操作HDFS，当HDFS没有启动时，Yarn无法操作HDFS，因此连接失败
# 解决方案：启动HDFS（start-dfs.sh）
input_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "up01:9092") \
    .option("subscribe", "tfec_access_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# 选择value字段信息（Nginx日志）
input_df = input_df.selectExpr("cast(value as string)")

"""
33.169.220.221 - - [13/Oct/2022:19:46:28 +0800] "GET /js/20.b9c086d.js HTTP/1.1" 200 159997 "-" "Mozilla/5.0 (X11; NetBSD) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.116 Safari/537.36" "-"

Nginx日志组成：
ip标识：33.169.220.221
用户标识：- -（cookie信息）
请求时间：[13/Oct/2022:19:46:28 +0800]
请求方式：GET
请求资源：/js/20.b9c086d.js
请求协议：HTTP/1.1
相应状态码：200
相应的数据大小：159997（字节）
请求来源："-"
useragent信息："Mozilla/5.0 (X11; NetBSD) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.116 Safari/537.36"
useraddr：代理地址
"""

# Java版本的正则表达式
regexp = '(?<ip>\d+\.\d+\.\d+\.\d+) (- - \[)(?<datetime>[\s\S]+)(?<t1>\][\s"]+)(?<request>[A-Z]+) (?<url>[\S]*) (?<protocol>[\S]+)["] (?<code>\d+) (?<sendbytes>\d+) ["](?<refferer>[\S]*) ["](?<useragent>[\S\s]+)["] ["](?<proxyaddr>[\S\s]+)["]'

# 使用正则表达式解析（匹配）Nginx日志数据
# regexp_extract(待匹配的字符串数据，正则表达式，索引号)
# 数据：日志数据
# 正则：Java版本的正则
# 索引号：从1开始
input_df = input_df.select(F.regexp_extract("value", regexp, 1).alias("ip"),
                           F.regexp_extract("value", regexp, 3).alias("datetime"),
                           F.regexp_extract("value", regexp, 5).alias("request"),
                           F.regexp_extract("value", regexp, 6).alias("url"),
                           F.regexp_extract("value", regexp, 7).alias("protocol"),
                           F.regexp_extract("value", regexp, 8).alias("code"),
                           F.regexp_extract("value", regexp, 9).alias("sendbytes"),
                           F.regexp_extract("value", regexp, 10).alias("refferer"),
                           F.regexp_extract("value", regexp, 11).alias("useragent"),
                           F.regexp_extract("value", regexp, 12).alias("proxyaddr"))


# 自定义的的函数，用于时间类型转换
@F.udf(returnType=StringType())
def parse_access_time(dateStr):
    dateStr = dateStr.replace(" +0800", "")
    # 中间结果对象
    dateObj = datetime.strptime(dateStr, '%d/%b/%Y:%H:%M:%S')
    # 目标格式：年-月-日 时:分:秒
    return dateObj.strftime('%Y-%m-%d %H:%M:%S')


# 时间格式转换
# withColumn("参数一","参数二"):
# 参数一：如果列名已存在，则会替换，如果列不存在，则会新增
input_df = input_df.withColumn("datetime", parse_access_time("datetime"))


# 自定义的UDF函数，用来解析IP地址
@F.udf(returnType=StringType())
def ip_to_address(ipStr):
    url = f"http://opendata.baidu.com/api.php?query={ipStr}&co=&resource_id=6006&oe=utf8"
    try:
        res = requests.request("GET", url).json()
        return res['data'][0]['location']
    except:
        print("-------------IP地址解析异常--------------")
        return "未知地址"


# IP地理位置解析函数
input_df = input_df.withColumn("area", ip_to_address("ip"))


# 解析UA信息，得到操作系统（os）、设备/品牌（device/brand）、浏览器信息（browser）
# 这个自定义函数，解析出来后，有3个信息，需要接受。PySpark又不支持一进多出（UDTF），只支持UDF。怎么办？
# 可以把这3个信息拼接成一个自付出，然后再切割。
@F.udf(returnType=StringType())
def parse_user_agent(useragent):
    userAgent = user_agents.parse(useragent)
    os = userAgent.os.family
    device = userAgent.device.family
    browser = userAgent.browser.family
    return f'{os},{device},{browser}'


input_df = input_df.withColumn("os", F.split(parse_user_agent("useragent"), ',')[0].alias("os")) \
    .withColumn("device", F.split(parse_user_agent("useragent"), ',')[1].alias("device")) \
    .withColumn("browser", F.split(parse_user_agent("useragent"), ',')[2].alias("browser"))

"""
需求：根据nginx日志，ip标识唯一的用户，需要ip分组，
统计得到用户访问的pv、uv、区域、状态码、终端设备的操作系统、设备品牌、浏览器、访问时间(年-月-日 时:分:秒)
"""

# 分组后必须聚合。这里由于有多个指标，所以必须使用agg聚合。
# F.first()：取聚合后的值
input_df = input_df.groupBy("ip") \
    .agg(F.count("ip").alias("pv"),
         F.lit(1).alias("uv"),
         F.first("area").alias("area"),
         F.first("code").alias("code"),
         F.first("os").alias("os"),
         F.first("device").alias("device"),
         F.first("browser").alias("browser"),
         F.first("datetime").alias("datetime"))

input_df.printSchema()

# 把结果输出到MySQL中，写入之前，把数据类型统一
input_df = input_df.selectExpr("ip",
                               "cast (pv as int)",
                               "uv",
                               "area",
                               "cast(code as int)",
                               "os",
                               "device",
                               "browser",
                               "datetime")

input_df.printSchema()

# 把数据打印到终端
query1 = input_df.writeStream.format("console").outputMode("complete")


# 怎么写MySQL？
def saveToMySQL(batch_df: DataFrame, batch_id):
    # batch_df可以直接写入MySQL
    batch_df.write.jdbc(url="jdbc:mysql://up01:3306/tfec_app",
                        table='nginx_access_result',
                        mode='overwrite',
                        properties={"user": "root", "password": "123456"})


# 写入到MySQL的任务
query2 = input_df.writeStream.outputMode("complete").foreachBatch(saveToMySQL)

# 启动流式任务
query1.start()
query2.start().awaitTermination()
