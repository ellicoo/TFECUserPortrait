from pyspark.sql import SparkSession, DataFrame
import os
from pyspark.sql import functions as F


from com.mytest.tag.base.AbstractBaseModel import AbstractBaseModel

"""
-------------------------------------------------
   Description :	TODO：
   SourceFile  :	MarriageStatusModel
   Author      :	mytest team
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'


class MarriageStatusModel(AbstractBaseModel):
    def compute(self, es_df: DataFrame, five_df: DataFrame):
        es_df.printSchema()
        es_df.show()
        # +---+--------+
        # | id|marriage|
        # +---+--------+
        # |  1|       2|
        # |  2|       2|
        # |  3|       2|
        # |  4|       1|
        # |  5|       3|
        five_df.printSchema()
        five_df.show()
        # +---+----+
        # | id|rule|
        # +---+----+
        # | 67|   1|
        # | 68|   2|
        # | 69|   3|
        # +---+----+


        # new_df = es_df.join(other=five_df, on=es_df['marriage'] == five_df['rule'], how='left') \
        #     .select(es_df['id'].alias("userId"), five_df['id'].alias("tagsId"))
        # 以下是等价书写方式

        # new_df = es_df.join(
        #     other=five_df,
        #     on=F.col('es_df.marriage') == F.col('five_df.rule'),
        #     how='left'
        # ).select(
        #     F.col('es_df.id').alias("userId"),
        #     F.col('five_df.id').alias("tagsId")
        # )
        # #'es_df.id'和'five_df.id'是为了避免歧义，但是这样join就无法执行了

        # 重命名es_df和five_df中的'id'列，以避免冲突
        es_df = es_df.withColumnRenamed('id', 'userId')
        five_df = five_df.withColumnRenamed('id', 'tagsId')

        # 现在可以安全地执行join操作，因为列名已经被重命名，不会冲突
        new_df = es_df.join(
            other=five_df,
            on=F.col('es_df.marriage') == F.col('five_df.rule'),
            how='left'
        ).select(
            'userId',  # es_df中的'id'列已重命名为'userId'
            'tagsId'  # five_df中的'id'列已重命名为'tagsId'
        )

        new_df.printSchema()
        new_df.show()
        return new_df


if __name__ == '__main__':
    marriageStatusModel = MarriageStatusModel(66)
    marriageStatusModel.execute()
