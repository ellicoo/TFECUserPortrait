from pyspark.sql import SparkSession, DataFrame
import os

from com.mytest.tag.base.AbstractBaseModel import AbstractBaseModel

"""
-------------------------------------------------
   Description :	TODO：政治面貌标签
   SourceFile  :	PoliticalFaceModel
   Author      :	mytest team
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'


class PoliticalFaceModel(AbstractBaseModel):
    def compute(self, es_df: DataFrame, five_df: DataFrame):
        es_df.printSchema()
        es_df.show()
        # +---+-------------+
        # | id|politicalface|
        # +---+-------------+
        # |  1|            1|
        # |  2|            3|
        # |  3|            3|
        # |  4|            1|
        # |  5|            1|
        five_df.printSchema()
        five_df.show()
        # +---+----+
        # | id|rule|
        # +---+----+
        # | 63|   1| 群众
        # | 64|   2| 党员
        # | 65|   3| 无党派人士
        # +---+----+

        new_df = es_df.join(other=five_df, on=es_df['politicalface'] == five_df['rule'], how='left') \
            .select(es_df['id'].alias("userId"), five_df['id'].alias("tagsId"))
        new_df.show()
        # +------+------+
        # |userId|tagsId|
        # +------+------+
        # |     6|    64|
        # |    10|    64|
        # |    11|    64|
        # |    26|    64|
        return new_df


if __name__ == '__main__':
    politicalFaceModel = PoliticalFaceModel(62)
    politicalFaceModel.execute()
