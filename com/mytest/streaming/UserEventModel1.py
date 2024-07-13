from pyspark.sql import SparkSession
import os

from com.mytest.streaming.base.StreamingBaseModel import StreamingBaseModel
import pyspark.sql.functions as F

"""
-------------------------------------------------
   Description :	TODO：用户行为模型重构
   SourceFile  :	UserEventModel1
   Author      :	mytest team
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'


class UserEventModel1(StreamingBaseModel):
    # 对数据进行ETL操作
    def etl_data(self, input_df):
        # 1.把value转换为string类型
        input_df = input_df.selectExpr("cast(value as string)")
        # 2.解析json数据
        input_df = input_df.select(
            F.json_tuple("value", "phone_num", "system_id", "user_name", "user_id", "visit_time", "goods_type",
                         "minimum_price") \
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

        input_df.printSchema()
        return input_df

    # 统计实时指标操作
    def compute(self, input_df):
        input_df = input_df.groupBy("user_id").agg(
            F.count(F.expr("if(is_browse = 1,user_id,null)")).alias("is_browse_cnt"),
            F.count(F.expr("if(is_order = 1,user_id,null)")).alias("is_order_cnt"),
            F.count(F.expr("if(is_buy = 1,user_id,null)")).alias("is_buy_cnt"),
            F.count(F.expr("if(is_back_order = 1,user_id,null)")).alias("is_back_order_cnt"),
            F.count(F.expr("if(is_received = 1,user_id,null)")).alias("is_received_cnt"))

        # 修改指标结果类型
        input_df = input_df.selectExpr("user_id",
                                       "cast(is_browse_cnt as int)",
                                       "cast(is_order_cnt as int)",
                                       "cast(is_buy_cnt as int)",
                                       "cast(is_back_order_cnt as int)",
                                       "cast(is_received_cnt as int)")
        input_df.printSchema()
        return input_df


if __name__ == '__main__':
    userEventModel = UserEventModel1(master='local[2]',
                                     appName='UserEventModel2',
                                     numPartitions=4,
                                     kafkaServers='up01:9092',
                                     offsets='earliest',
                                     topic='tfec_user_event',
                                     database_name='tfec_app',
                                     table_name='user_event_result1',
                                     outputMode='complete')
    userEventModel.execute()
