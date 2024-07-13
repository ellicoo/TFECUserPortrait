from pyspark.sql import SparkSession, DataFrame, Window
import os

from com.mytest.tag.base.AbstractBaseModel import AbstractBaseModel
import pyspark.sql.functions as F

"""
-------------------------------------------------
   Description :	TODO：支付方式标签代码
   SourceFile  :	PayWayModel
   Author      :	mytest team
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'


class PayWayModel(AbstractBaseModel):
    def compute(self, es_df: DataFrame, five_df: DataFrame):
        es_df.printSchema()
        es_df.show()
        # +--------+-----------+
        # |memberid|paymentcode|
        # +--------+-----------+
        # |     342|     alipay|
        # |     405|     alipay|
        # |     653|     alipay|
        # |     504|        cod|
        # 对业务数据进行分组聚合，取每组中支付方式的最大值
        # F.row_number().over(Window.partitionBy("memberid").orderBy(F.desc("paymentcode_cnt")))
        # row_number()：行号
        # over：开窗函数
        # Window：DSL中开窗函数需要通过Window对象来调用
        es_df: DataFrame = es_df.groupBy("memberid", "paymentcode").agg(F.count("memberid").alias("paymentcode_cnt")) \
            .select("memberid",
                    "paymentcode",
                    "paymentcode_cnt",
                    F.row_number().over(Window.partitionBy("memberid").orderBy(F.desc("paymentcode_cnt"))).alias("rn")) \
            .where("rn = 2") \
            .select("memberid", "paymentcode")
        es_df.printSchema()
        es_df.show()
        five_df.printSchema()
        five_df.show()
        # +---+--------+
        # | id|    rule|
        # +---+--------+
        # | 31|  alipay|
        # | 32|   wxpay|
        # | 33|chinapay|
        # | 34|  kjtpay|
        # | 35|     cod|
        # | 36|   other|
        # +---+--------+
        # 把es_df和five_df进行关联
        new_df = es_df.join(other=five_df, on=es_df['paymentcode'] == five_df['rule'], how='left') \
            .select(es_df['memberid'].alias("userId"), five_df['id'].alias("tagsId"))
        new_df.printSchema()
        new_df.show()
        return new_df


if __name__ == '__main__':
    payWayModel = PayWayModel(30)
    payWayModel.execute()
