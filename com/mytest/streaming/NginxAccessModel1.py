from datetime import datetime

import requests
import user_agents
from pyspark.sql import SparkSession
import os

from pyspark.sql.types import StringType

from com.mytest.streaming.base.StreamingBaseModel import StreamingBaseModel
import pyspark.sql.functions as F

"""
-------------------------------------------------
   Description :	TODO：NginxAccess访问基类重构
   SourceFile  :	NginxAccessModel1
   Author      :	mytest team
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'


# 自定义的的函数，用于时间类型转换
@F.udf(returnType=StringType())
def parse_access_time(dateStr):
    dateStr = dateStr.replace(" +0800", "")
    # 中间结果对象
    dateObj = datetime.strptime(dateStr, '%d/%b/%Y:%H:%M:%S')
    # 目标格式：年-月-日 时:分:秒
    return dateObj.strftime('%Y-%m-%d %H:%M:%S')


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


@F.udf(returnType=StringType())
def parse_user_agent(useragent):
    userAgent = user_agents.parse(useragent)
    os = userAgent.os.family
    device = userAgent.device.family
    browser = userAgent.browser.family
    return f'{os},{device},{browser}'


class NginxAccessModel1(StreamingBaseModel):
    # 1.对数据进行ETL操作
    def etl_data(self, input_df):
        # 1.对Kafka的数据进行转换
        input_df = input_df.selectExpr("cast(value as string)")
        # 定义正则表达式
        regexp = '(?<ip>\d+\.\d+\.\d+\.\d+) (- - \[)(?<datetime>[\s\S]+)(?<t1>\][\s"]+)(?<request>[A-Z]+) (?<url>[\S]*) (?<protocol>[\S]+)["] (?<code>\d+) (?<sendbytes>\d+) ["](?<refferer>[\S]*) ["](?<useragent>[\S\s]+)["] ["](?<proxyaddr>[\S\s]+)["]'
        # 正则处理
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

        # 2.转换时间
        input_df = input_df.withColumn("datetime", parse_access_time("datetime"))

        # 3.IP解析
        input_df = input_df.withColumn("area", ip_to_address("ip"))

        # 4.UA解析
        input_df = input_df.withColumn("os", F.split(parse_user_agent("useragent"), ',')[0].alias("os")) \
            .withColumn("device", F.split(parse_user_agent("useragent"), ',')[1].alias("device")) \
            .withColumn("browser", F.split(parse_user_agent("useragent"), ',')[2].alias("browser"))

        input_df.printSchema()

        return input_df

    # 对数据进行统计分析
    def compute(self, input_df):
        input_df = input_df.groupBy("ip") \
            .agg(F.count("ip").alias("pv"),
                 F.lit(1).alias("uv"),
                 F.first("area").alias("area"),
                 F.first("code").alias("code"),
                 F.first("os").alias("os"),
                 F.first("device").alias("device"),
                 F.first("browser").alias("browser"),
                 F.first("datetime").alias("datetime"))

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
        return input_df


if __name__ == '__main__':
    # 20221013：10条，offset=10
    # 20221015：10条，offset=10 + 10 = 20
    # 20221014：16678条，offset=16678 + 20 = 16698
    nginxAccess = NginxAccessModel1(master='local[2]',
                                    appName='NginxAccessModel1',
                                    numPartitions=4,
                                    kafkaServers='up01:9092',
                                    offsets='{"tfec_access_topic":{"0":16678}}',
                                    topic='tfec_access_topic',
                                    database_name='tfec_app',
                                    table_name='nginx_access_result1',
                                    outputMode='complete')
    nginxAccess.execute()
