from pyspark.sql import SparkSession, DataFrame
import os

from com.mytest.tag.base.AbstractBaseModel import AbstractBaseModel

"""
-------------------------------------------------
   Description :	TODO：职业标签代码
   SourceFile  :	JobModel
   Author      :	mytest team
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'


class JobModel(AbstractBaseModel):
    def compute(self, es_df: DataFrame, five_df: DataFrame):
        es_df.printSchema()
        es_df.show()
        # +---+---+
        # | id|job|
        # +---+---+
        # |  1|  3|
        # |  2|  3|
        # |  3|  1|
        # |  4|  3|
        # |  5|  3|
        five_df.printSchema()
        five_df.show()
        # +---+----+
        # | id|rule|
        # +---+----+
        # |  8|   1|
        # |  9|   2|
        # | 10|   3|
        # | 11|   4|
        # | 12|   5|
        # | 13|   6|
        # +---+----+
        new_df = es_df.join(other=five_df, on=es_df['job'] == five_df['rule'], how='left') \
            .select(es_df['id'].alias("userId"), five_df['id'].alias("tagsId"))
        new_df.show()
        # +------+------+
        # |userId|tagsId|
        # +------+------+
        # |     3|     8|
        # |     6|     8|
        # |    14|     8|
        # |    15|     8|
        return new_df


if __name__ == '__main__':
    jobModel = JobModel(7)
    jobModel.execute()
