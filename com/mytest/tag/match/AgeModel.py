from pyspark.sql import SparkSession, DataFrame
import os

from pyspark.sql.types import StringType

from com.mytest.tag.bean.EsMeta import tagRuleStrToEsMeta, EsMeta
import pyspark.sql.functions as F

"""
-------------------------------------------------
   Description :	TODO：年龄段标签代码
   SourceFile  :	AgeModel
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
    .appName("SparkSQLAppName") \
    .config("spark.sql.shuffle.partitions", 4) \
    .getOrCreate()

"""
todo：看起来的效果是高亮，实际上在工作中的含义是未完成的功能。待后续开发。
"""
# todo step1：从MySQL中读取四级标签年龄段的数据，并获取rule，提取要读取的ES中的数据内容，转换成字典
# 读取四级标签年龄段数据
input_df = spark.read.jdbc(url='jdbc:mysql://up01:3306/tfec_tags',
                           table='tbl_basic_tag',
                           properties={"user": "root", "password": "123456"})
# input_df.printSchema()
# input_df.show(truncate=False)
# 获取rule规则
four_df: DataFrame = input_df.where("id = 14").select("rule")
four_df.printSchema()
four_df.show(truncate=False)
# fist:action算子，可以把数据收集到Driver端
# ['rule']或者[0]：获取第一列的值
# print(four_df.first()['rule'])
ruleStr = four_df.first()[0]
# 把规则转换为字典
ruleDict = tagRuleStrToEsMeta(ruleStr)
# print(ruleDict)
# 把规则字典转换为EsMeta对象
esMeta = EsMeta(**ruleDict)
# print(esMeta)


# todo step2：根据step1中的信息，读取ES中需要用到的业务数据：用户的id、用户的出生日期birthday，去掉分隔符
# es.read.field.include：选择读取哪些字段
# es.mapping.date.rich：修改es默认的配置，禁止把时间日期类型的数据转换为时间日期格式（目的是为了保留原本的格式）
es_df = spark.read \
    .format("es") \
    .option("es.resource", esMeta.esIndex) \
    .option("es.nodes", esMeta.esNodes) \
    .option("es.read.field.include", esMeta.selectFields) \
    .option("es.mapping.date.rich", "false") \
    .load()
# 去掉横杠(-)
# regexp_replace(待操作的字符串, 需要替换的字符串, 替换后的样子)
user_df = es_df.select("id", F.regexp_replace("birthday", "-", "").alias("birthday"))
# es_df.printSchema()
# es_df.show()
user_df.printSchema()
user_df.show()

# todo step3：从MySQL中读取五级标签年龄段所有的值，并拆解成start和end范围
five_df: DataFrame = input_df.where("pid = 14") \
    .select("id",
            F.split("rule", "-")[0].alias("start"),
            F.split("rule", "-")[1].alias("end"))
five_df.printSchema()
five_df.show()

# todo step4：根据ES中用户的birthday对比五级标签，标记每个用户的年龄段标签
# join(other=其他的DF, on=join的条件, how=join的方式，默认是inner)
# where("birthday between start and end")
result_df: DataFrame = user_df.join(other=five_df) \
    .where(user_df['birthday'].between(five_df['start'], five_df['end'])) \
    .select(user_df['id'].alias("userId"), five_df['id'].alias("tagsId").cast(StringType()))
result_df.printSchema()
result_df.show()

# todo step5：将每个用户的年龄段标签数据写入ES中
# es.mapping.id:userId，使用userId来代替es中的映射的id，如果不知道，会自动生成随机的ID
result_df.write.format("es") \
    .option("es.resource", "tags_result") \
    .option("es.nodes", esMeta.esNodes) \
    .option("es.mapping.id", "userId") \
    .mode("append") \
    .save()

# 关闭SparkSession
spark.stop()
