from pyspark.sql import SparkSession, DataFrame
import os

from pyspark.sql.types import StringType

from com.mytest.tag.base.AbstractBaseModel import AbstractBaseModel
import pyspark.sql.functions as F

"""
-------------------------------------------------
   Description :	TODO：
   SourceFile  :	AgeModel1
   Author      :	mytest team
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'


class AgeModel1(AbstractBaseModel):
    # 如果子类没有显式声明自己的 __init__ 方法，那么在创建子类实例时，父类的 __init__ 方法会被自动调用
    # 如果子类自定义自己的魔法函数，则需要super().__init__(...)调用父类构造器
    # Ctrl + O:快速打开覆盖选项
    # 打标签的方法，每个标签的逻辑都不一样，因此每个标签都需要自己实现这个方法
    def compute(self, es_df, five_df):
        # es_df：业务数据
        # |1992-05-31|  1|
        # |1983-10-11|  2|
        # |1970-11-22|  3|
        user_df = es_df.select("id", F.regexp_replace("birthday", "-", "").alias("birthday"))
        # five_df：标签规则数据
        # | 15|19500101-19591231|
        # | 16|19600101-19691231|
        five_df: DataFrame = five_df.select("id",
                                            F.split("rule", "-")[0].alias("start"),
                                            F.split("rule", "-")[1].alias("end"))
        result_df: DataFrame = user_df.join(other=five_df) \
            .where(user_df['birthday'].between(five_df['start'], five_df['end'])) \
            .select(user_df['id'].alias("userId"), five_df['id'].alias("tagsId").cast(StringType()))
        result_df.printSchema()
        result_df.show()
        return result_df


if __name__ == '__main__':
    # 14:表示年龄段标签的ID号
    ageModel = AgeModel1(14)
    ageModel.execute()
