from pyspark.sql import SparkSession, DataFrame
import os
import pyspark.sql.functions as F

"""
-------------------------------------------------
   Description :	TODO：用户行为数据代码
   SourceFile  :	UserEventModel
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

input_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "up01:9092") \
    .option("startingOffsets", "earliest") \
    .option("subscribe", "tfec_user_event") \
    .load()
# 在 Kafka 中，数据以消息（message）的形式存储和传递。每条消息包含两个主要部分：key 和 value
# 读出来的是一个名叫input_df的dataframe,每行数据都是一个Kafka 的 value，
# value可以是任意格式，具体取决于生产者发送的数据格式。常见的格式包括纯文本（如字符串日志行）、JSON、Avro、Protobuf 等。
# 本例子中Kafka的键值对消息的value是个json格式，需要通过selectExpr算子的SQL表达式将value转类型为字符串string
input_df = input_df.selectExpr("cast(value as string)")

"""
{
	"phone_num": "15149869873",
	"system_id": "YR56X0005TI",
	"area": {
		"province": "内蒙古",
		"city": "巴彦淖尔",
		"sp": "移动"
	},
	"user_name": "张秀珍",
	"user_id": "1046-5506849",
	"visit_time": "2022-10-13 19:46:22",
	"goods_type": "水家电",
	"minimum_price": 5637.2,
	"user_behavior": {
		"is_browse": 1,
		"is_order": 0,
		"is_buy": 0,
		"is_back_order": 0,
		"is_received": 0
	},
	"goods_detail": {
		"goods_name": "Haier/海尔燃气热水器JSQ31-16TE3(12T)",
		"browse_page": "https://www.tfec.com/televisions/?filter=1289&spm=cn.30673_pc.header_1_20210207.1",
		"browse_time": "2022-10-13 19:21:22",
		"to_page": "https://www.tfec.com/laundry/20220620_182613.shtml?spm=cn.33996_pc.product_20220520.16",
		"to_time": "2022-10-13 19:46:22",
		"page_keywords": "破壁机"
	}
}
"""

# json解析
input_df = input_df.select(
    F.json_tuple("value", "phone_num", "system_id", "user_name", "user_id", "visit_time", "goods_type", "minimum_price")
    .alias("phone_num", "system_id", "user_name", "user_id", "visit_time", "goods_type", "minimum_price"),
    F.get_json_object("value", "$.area.province").alias("province"),
    F.get_json_object("value", "$.area.city").alias("city"),
    F.get_json_object("value", "$.area.sp").alias("sp"),
    F.get_json_object("value", "$.user_behavior.is_browse").alias("is_browse"),
    F.get_json_object("value", "$.user_behavior.is_order").alias("is_order"),
    F.get_json_object("value", "$.user_behavior.is_buy").alias("is_buy"),
    F.get_json_object("value", "$.user_behavior.is_back_order").alias("is_back_order"),
    F.get_json_object("value", "$.user_behavior.is_received").alias("is_received"),
    F.get_json_object("value", "$.goods_detail.goods_name").alias("goods_name"),
    F.get_json_object("value", "$.goods_detail.browse_page").alias("browse_page"),
    F.get_json_object("value", "$.goods_detail.browse_time").alias("browse_time"),
    F.get_json_object("value", "$.goods_detail.to_page").alias("to_page"),
    F.get_json_object("value", "$.goods_detail.to_time").alias("to_time"),
    F.get_json_object("value", "$.goods_detail.page_keywords").alias("page_keywords"))

"""
需求：
    统计每个用户浏览访问的总页面数 
    统计每个用户的下单行为的总数
    统计每个用户支付行为总数
    统计每个用户退单行为总数
    统计每个用户主动点击收货行为总数
"""

# select()
# selectExpr()
# expr(sql)
# count:可以统计结果为0的值，但是不会统计结果为null的值。
# sum:不会统计结果为null和为0的值

# DSL中的两个类SQL操作的比较：
# selectExpr()--单独使用SQL操作
# function中的expr()--在DSL的统计操作中使用SQL语句操作
input_df = input_df.groupBy("user_id").agg(F.count(F.expr("if(is_browse = 1,user_id,null)")).alias("is_browse_cnt"),
                                           F.count(F.expr("if(is_order = 1,user_id,null)")).alias("is_order_cnt"),
                                           F.count(F.expr("if(is_buy = 1,user_id,null)")).alias("is_buy_cnt"),
                                           F.count(F.expr("if(is_back_order = 1,user_id,null)")).alias(
                                               "is_back_order_cnt"),
                                           F.count(F.expr("if(is_received = 1,user_id,null)")).alias("is_received_cnt"))

# 修改指标结果类型
input_df = input_df.selectExpr("user_id",
                               "cast(is_browse_cnt as int)",
                               "cast(is_order_cnt as int)",
                               "cast(is_buy_cnt as int)",
                               "cast(is_back_order_cnt as int)",
                               "cast(is_received_cnt as int)")

input_df.printSchema()


# 采用批量写入到MySQL的方式
def saveToMySQL(batch_df: DataFrame, batch_id):
    batch_df.write.jdbc(url='jdbc:mysql://up01:3306/tfec_app',
                        table='user_event_result',
                        mode='overwrite',
                        properties={"user": "root", "password": "123456"})


# 写出结果到MySQL中
input_df.writeStream.outputMode("complete").foreachBatch(saveToMySQL).start()

# 结果输出到console
input_df.writeStream.format("console").outputMode("complete").start().awaitTermination()
