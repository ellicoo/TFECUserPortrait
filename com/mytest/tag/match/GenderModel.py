from pyspark.sql import SparkSession, DataFrame
import os

from pyspark.sql.types import StringType

from com.mytest.tag.bean.EsMeta import tagRuleStrToEsMeta, EsMeta
import pyspark.sql.functions as F

"""
-------------------------------------------------
   Description :	TODO：性别标签的代码
   SourceFile  :	GenderModel
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

# todo step1：从MySQL中读取四级标签性别的数据，并获取rule，提取要读取的ES中的数据内容转换成字典
input_df = spark.read.jdbc(url='jdbc:mysql://up01:3306/tfec_tags',
                           table='tbl_basic_tag',
                           properties={"user": "root", "password": "123456"})
ruleStr = input_df.where("id = 4").select("rule").first()[0]
esMeta = EsMeta(**tagRuleStrToEsMeta(ruleStr))

# print(ruleStr)
# print(esMeta)

# todo step2：根据step1中的信息，读取ES中需要用到的业务数据：用户的id、用户的性别gender
es_df = spark.read \
    .format('es') \
    .option("es.resource", esMeta.esIndex) \
    .option("es.nodes", esMeta.esNodes) \
    .option("es.read.field.include", esMeta.selectFields) \
    .option("es.mapping.date.rich", "false") \
    .load()
# es_df.printSchema()
# es_df.show()

# todo step3：从MySQL中读取五级标签年龄段所有的值
five_df: DataFrame = input_df.where("pid = 4").select("id", "rule")
# five_df.printSchema()
# five_df.show()

# todo step4：根据ES中用户的gender对比五级标签，标记每个用户的性别标签
new_df = es_df.join(other=five_df, on=es_df['gender'] == five_df['rule'], how='inner') \
    .select(es_df['id'].alias("userId"), five_df['id'].alias("tagsId"))
new_df.printSchema()
new_df.show()

# todo 不合并标签，直接写到es中的效果
# result_df.write\
#     .format("es")\
#     .option("es.resource","tags_result")\
#     .option("es.nodes",esMeta.esNodes)\
#     .option("es.mapping.id","userId")\
#     .mode("append")\
#     .save()

# todo step5：将tags_result标签库中的数据读取出来（old_df)
old_df = spark.read \
    .format('es') \
    .option("es.resource", "tags_result") \
    .option("es.nodes", esMeta.esNodes) \
    .option("es.read.field.include", "userId,tagsId") \
    .option("es.mapping.date.rich", "false") \
    .load()
old_df.printSchema()
old_df.show()


# todo step6：把标签库中的历史标签数据(old_df)和新的标签数据(new_df)进行合并
# 自定义的函数，用来实现标签的合并
@F.udf(returnType=StringType())
def merge_tags(new_df, old_df):
    # 1.如果new_df为空，返回old_df数据
    if new_df is None:
        return old_df
    # 2.如果old_df为空，返回new_df数据
    if old_df is None:
        return new_df

    # 3.如果两个都不为空，实现标签的合并
    # 3.1 new_df切割，得到一个列表，new_df_list
    new_df_list = str(new_df).split(",")
    # 3.2 old_df切割，得到一个列表，old_df_list
    old_df_list = str(old_df).split(",")
    # 3.3 把new_df_list和old_df_list进行合并，得到result_df_list
    result_df_list = new_df_list + old_df_list
    # 3.4 把最终的result_df_list以固定的符号拼接，返回
    return ",".join(set(result_df_list))


result_df = new_df.join(other=old_df, on=new_df['userId'] == old_df['userId'], how='left') \
    .select(new_df['userId'], merge_tags(new_df['tagsId'], old_df['tagsId']).alias("tagsId"))
result_df.printSchema()
result_df.show()

# todo step7：将合并后的标签结果写入到ES中
result_df.write \
    .format("es") \
    .option("es.resource", "tags_result") \
    .option("es.nodes", esMeta.esNodes) \
    .option("es.mapping.id", "userId") \
    .mode("append") \
    .save()

# 关闭SparkSession
spark.stop()
