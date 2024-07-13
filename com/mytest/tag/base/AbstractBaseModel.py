from pyspark.sql import SparkSession, DataFrame
import os

from pyspark.sql.types import StringType

from com.mytest.tag.bean.EsMeta import EsMeta, tagRuleStrToEsMeta
import pyspark.sql.functions as F

"""
-------------------------------------------------
   Description :	TODO：基类（父类）
   SourceFile  :	AbstractBaseModel
   Author      :	mytest team
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'



"""
#################步骤一：把公共方法抽取到父类中#################
#0.初始化Spark环境
#1.根据4级标签ID，读取MySQL标签体系数据
#2.过滤4级标签的数据，将四级标签的rule转换为esMeta对象（需要4级标签的id参数，子类传递）
#3.根据esMeta对象从ES中读取相应的业务数据
#4.根据4级标签ID，读取5级标签的数据（需要根据4级标签的id参数，子类传递）
#5.通过ES中的业务数据与MySQL的5级标签rule规则进行打5级标签的id作为标签（子类实现）
#6.从ES中读取历史用户标签数据
#7.将老的用户画像标签与新的标签进行合并，得到最终标签
#8.将最终的结果写入ES中
#9.销毁Spark环境，释放资源
#################步骤二：定义一个父类的成员方法，把前面的基础方法都串联起来#################

# 操作的数据源(业务数据表)：
# 要给哪些表打标签：1）tbl_goods、2）tbl_logs、3) tbl_orfers、4) tbl_users打标签
# 用户有订单表--tbl_orders
# 用户有访问日志表--tbl_logs
# 用户有账户表--tbl_users
# 商品表--tbl_goods

# 不管你是给tbl_goods、tbl_logs还是tbl_orfers、tbl_users打标签，所有打标签结果都追加到tags_result这个tags_result的index中，使用append的方式
# 要先保证这个几个表的id是对应的，商品表的userId则


"""
# 自定义的函数，用来实现标签的合并
@F.udf(returnType=StringType())
def merge_tags(new_df, old_df, fiveTagIDStr):
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

    # fiveTagIDStr字符串中，包含了所有5级标签的ID，使用(,)拼接，因此需要使用(,)切割

    # 因为可能以前userId打的是24这个标签--old_df的tag:24，现在更新了打25这个标签--new_df的tag:25，如果仅仅进行new_df的tag和old_df的tag进行合并去重就会出现互相矛盾的问题，
    # 比如访问周期，就会出现，即是0～7这个访问周期，又是7～14这个访问周期，而应该只保留最新的7～14这个tag周期。

    five_tag_id_list = fiveTagIDStr.split(",")
    # 通过当前这个4级标签种类(因为4级标签种类很多)下的所有5级标签，将原来的已经打的这个4级标签种类下的所有旧的5级标签全部剔除，old_df_list只保留其他4级标签种类的所有5级标签
    # 将以前的同类标签拔除，保留其他类别标签
    for tag in five_tag_id_list:
        if tag in old_df_list:
            old_df_list.remove(tag)

    # 3.3 把new_df_list和old_df_list进行合并，得到result_df_list
    result_df_list = new_df_list + old_df_list
    # 3.4 把最终的result_df_list以固定的符号拼接，返回
    return ",".join(set(result_df_list))


class AbstractBaseModel:
    # 0.初始化Spark环境
    # 传入想要操作的指定某个四级标签的ID
    def __init__(self, fourTagId):
        # 构建SparkSession
        # 建造者模式：类名.builder.配置…….getOrCreate()
        # 自动帮你构建一个SparkSession对象，只要指定你需要哪些配置就可

        # 此处使用的但是对象类型的变量(私有变量)，不是类的变量(公共变量)
        self.fourTagId = fourTagId
        self.spark = SparkSession \
            .builder \
            .master("local[2]") \
            .appName("SparkSQLAppName") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.shuffle.partitions", 4) \
            .getOrCreate()

    # 1.根据4级标签ID，读取MySQL整个标签体系数据，-- 这是一张标签体系表，将mysql的数据读成spark的dataframe表
    # 整个标签体系是细粒度规则和粗粒度规则都用一个字段存储，使用子母类id与pid进行树的数据结构的封装
    def read_mysql_data(self):
        input_df = self.spark.read.jdbc(url='jdbc:mysql://up01:3306/tfec_tags',
                                        table='tbl_basic_tag',
                                        properties={"user": "root", "password": "123456"})
        return input_df

    # 2.过滤4级标签的数据，将四级标签的rule转换为esMeta对象，id不同，逻辑相同--
    # 将整个标签规则体系表input_df，找出本次我们需要给哪个es表打标签--即业务数据，这个数据源的存储位置，存储在本次指定的4级标签id对应的4级标签rule规则中
    # 参数说明：
    # input_df--整个标签规则体系表
    #           通过调用的具体对象参数fourTagId--指定本次操作的leve=4的4级标签的id找出4级标签规则--
    #           4级标签规则是粗粒度规则--告诉你在哪里操作
    #
    def input_df_to_esMeta(self, input_df):
        # first() 返回的是一个 Row 对象，包含了该行所有列的值--4级标签的规则rule确定了要操作的哪个es数据源头和哪些必要字段
        # 主要是对粗粒度的规则进行解析，目的为了找到我们需要操作的es中的哪个数据表
        ruleStr = input_df.where(f"id = {self.fourTagId}").select("rule").first()[0]
        esMeta = EsMeta(**tagRuleStrToEsMeta(ruleStr))
        return esMeta

    # 3.根据esMeta对象从ES中读取相应的业务数据，一样的
    #  参数说明：
    # esMeta信息--是包装出的新类型的对象--是因为预判4级标签规则里面指定的数据源的储存位置信息，切割成需要的几个参数信息，
    #            用对象存储起来--方便复用，比如read_old_df_from_es和write_result_df_to_es就再需要调用
    def read_es_df_by_esMeta(self, esMeta):
        es_df = self.spark.read \
            .format('es') \
            .option("es.resource", esMeta.esIndex) \
            .option("es.nodes", esMeta.esNodes) \
            .option("es.read.field.include", esMeta.selectFields) \
            .option("es.mapping.date.rich", "false") \
            .load()
        return es_df

    # 4.根据4级标签ID，读取5级标签的数据，一样的
    def read_five_df_by_fourTagId(self, input_df):
        # 通过4级标签id与pid的匹配，找出在某个4级标签id下的所有5级规则--5级标签规则是细粒度的规则rule
        # 因为compute方法需要five_df的rule字段，里面存了5级标签的规则rule，compute方法需要根据这个规则打标签
        five_df: DataFrame = input_df.where(f"pid = {self.fourTagId}").select("id", "rule")
        return five_df

    # 5.通过ES中的业务数据与MySQL的5级标签进行打标签，完全不一样，返回new_df
    def compute(self, es_df, five_df):
        pass

    # 6.从ES中读取历史用户标签数据，一样的
    def read_old_df_from_es(self, esMeta):
        old_df = self.spark.read \
            .format('es') \
            .option("es.resource", "tags_result") \
            .option("es.nodes", esMeta.esNodes) \
            .option("es.read.field.include", "userId,tagsId") \
            .option("es.mapping.date.rich", "false") \
            .load()
        return old_df

    # 7.将老的用户画像标签与新的标签进行合并，得到最终标签，一样的
    # es不好进行join操作，但是spark很好join。
    # 根据esmeta的对象信息，将es的数据读成spark的dataframe，再使用spark的dataframe进行join操作，是很棒的，避免es不擅长join操作的问题

    def merge_old_df_and_new_df(self, new_df, old_df, fiveTagIDStr):
        result_df = new_df.join(other=old_df, on=new_df['userId'] == old_df['userId'], how='left') \
            .select(new_df['userId'], merge_tags(new_df['tagsId'], old_df['tagsId'], fiveTagIDStr).alias("tagsId"))
        return result_df

    # 8.将最终的结果写入ES中，一样的
    # result_df是新的打标签的结果表，假如原表中的userId的标签在result_df这个新df中被更新，append则会覆盖原表的tag标签，否则新增
    # 假如文档 ID 唯一，Elasticsearch 的 append 操作可以包括新增和更新

    # 不管你是给tbl_goods、tbl_logs还是tbl_orfers、tbl_users打标签，所有打标签结果都追加到tags_result这个tags_result的index中，使用append的方式
    # 要先保证这个几个表的id是对应的，或者完全不同的
    # 用户有订单表--tbl_orders
    # 用户有访问日志表--tbl_logs
    # 用户有账户表--tbl_users
    # 商品表--tbl_goods
    # 目标工作： 给这几个表的关键id打标签


def write_result_df_to_es(self, result_df, esMeta):
    result_df.write \
        .format("es") \
        .option("es.resource", "tags_result") \
        .option("es.nodes", esMeta.esNodes) \
        .option("es.mapping.id", "userId") \
        .mode("append") \
        .save()

    # 9.销毁Spark环境，释放资源，一样的


def close(self):
    self.spark.stop()


"""
自定义的execute方法，用于把上述的方法串起来执行
"""


def execute(self):
    # # 0.初始化Spark环境
    self.__init__(self.fourTagId)
    # 1.根据4级标签ID，读取MySQL标签体系数据--读出来的结果是一张表
    input_df = self.read_mysql_data()
    # 2.过滤4级标签的数据，将四级标签的rule转换为esMeta对象，id不同，逻辑相同
    esMeta = self.input_df_to_esMeta(input_df)
    # 3.根据esMeta对象从ES中读取相应的业务数据--读出来的结果是一张表--一张待匹配或者待统计或者待挖掘的业务数据的表
    es_df = self.read_es_df_by_esMeta(esMeta)
    # 4.根据4级标签ID，读取5级标签的数据
    # 刚刚我们只要input(来源与mysql)表中的rule字段来转换成meta对象去es中找待操作的数据表
    # 现在我们还需要input表中的其他字段值，level字段为5的值，这个值有很多，但我只要刚刚指定的rule字段下的5级值，取出的是规则值
    # 把规则值和刚刚找到的es表的数据进行操作(匹配、统计、挖掘)
    five_df = self.read_five_df_by_fourTagId(input_df)
    # 5.通过ES中的业务数据与MySQL的5级标签进行打标签，完全不一样
    new_df = self.compute(es_df, five_df)
    try:
        # 6.从ES中读取历史用户标签数据
        old_df = self.read_old_df_from_es(esMeta)
        # 标签更新
        """
        需要传入改4级标签下的所有5级标签的ID号。不能直接给five_df（ID、rule）
        （1）通过five_df获取所有ID
        （2）传入所有ID（List）List不能直接传入到自定义函数中，自定义需要只能传入2种类型（Column、Str）
        （3）把list转换为字符串，使用固定的分割符号拼接
            ",".join(List)
        """
        fiveTagIDList = five_df.rdd.map(lambda x: x.id).collect()

        # 7.将老的用户画像标签与新的标签进行合并，得到最终标签

        """
        ",".join(str(id) for id in fiveTagIDList) 首先将 fiveTagIDList 中的所有 ID 转换为字符串，并用逗号连接成一个字符串。
        F.lit() 然后将这个生成的字符串转换为 Spark 列对象，这个列对象的值在 DataFrame 的每一行中都是相同的
        """
        result_df = self.merge_old_df_and_new_df(new_df, old_df, F.lit(",".join(str(id) for id in fiveTagIDList)))
    except:
        # 如果出了问题，说明这个任务是第一次运行，没有历史标签库
        # 可以使用new_df当做标签结果数据，避免合并出问题
        result_df = new_df
        print("--------------首次执行，跳过合并---------------")
    # 8.将最终的结果写入ES中
    self.write_result_df_to_es(result_df, esMeta)
    # 9.销毁Spark环境，释放资源
    self.close()
