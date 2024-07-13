from pyspark.sql import SparkSession
import os

from com.mytest.tag.base.AbstractBaseModel import AbstractBaseModel

"""
-------------------------------------------------
   Description :	TODO：
   SourceFile  :	GenderModel1
   Author      :	mytest team
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'


class GenderModel1(AbstractBaseModel):
    def compute(self, es_df, five_df):
        # es_df.printSchema()
        # es_df.show()
        # five_df.printSchema()
        # five_df.show()
        new_df = es_df.join(other=five_df, on=es_df['gender'] == five_df['rule'], how='inner') \
            .select(es_df['id'].alias("userId"), five_df['id'].alias("tagsId"))
        new_df.printSchema()
        new_df.show()
        return new_df


if __name__ == '__main__':
    genderModel = GenderModel1(4)
    genderModel.execute()
