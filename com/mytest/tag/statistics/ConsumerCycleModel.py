from pyspark.sql import SparkSession, DataFrame
import os

from com.mytest.tag.base.AbstractBaseModel import AbstractBaseModel
import pyspark.sql.functions as F

"""
-------------------------------------------------
   Description :	TODO：统计类标签-消费周期
   SourceFile  :	ConsumerCycleModel
   Author      :	mytest team
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'


# 不管你是给tbl_goods、tbl_logs还是tbl_orfers、tbl_users打标签，所有打标签结果都追加到tags_result这个tags_result的index中，使用append的方式
# 要先保证这个几个表的id是对应的，或者完全不同的
# 用户有订单表--tbl_orders  -- 打标签是为了统计或者评价用户的购买力
# 用户有访问日志表--tbl_logs -- 打标签是为分析用户的访问频率等
# 用户有账户表--tbl_users -- 打标签目的是为了给客人打性别年龄等用户画像标签
# 以上三表要保证必要id字段值一致，且与商品表的平台id--cOrderSn不同值，否则打标签混乱

# 商品表--tbl_goods：打标签的目的是分析哪个平台的都销售了哪些产品和类型--是平台打标签分析
# 目标工作： 给这几个表的关键id打标签

class ConsumerCycleModel(AbstractBaseModel):
    # 这是消费周期标签类--是AbstractBaseModel的子类
    # 继承了AbstractBaseModel所有成员方法，但子类不会自动继承父类的构造方法（__init__方法）
    # 但是fourTagId是父类必须要初始化指定的

    # 子类 ConsumerCycleModel 没有定义自己的构造方法（即 __init__ 方法），
    # Python 将自动调用父类 AbstractBaseModel 的构造方法。这意味着只要在实例化 ConsumerCycleModel 时传递正确的参数，就会传递给父类的构造方法
    # 因为父类的构造方法会被自动调用，从而fourTagId 会被正确初始化

    # compute参数说明：
    # es_df--是read_es_df_by_esMeta根据4级标签规则数据(找到在mysql的整个标签规则数据中的指定的某个4级标签规则)解析出的esMeta信息--
    #        esMeta信息存储了es中需要操作的目标数据源的位置和哪些必要字段，通过这个esMeta信息将es的数据表读成spark的dataframe
    # five_df--存储了需要打的leve=5的5级标签id,和5级标签规则rule，5级标签ID是我们需要打的标签，5级标签规则rule是指明我们需要通过什么标准打这个标签id标签
    def compute(self, es_df: DataFrame, five_df: DataFrame):
        # execute()方法中的es_df = self.read_es_df_by_esMeta(esMeta)
        es_df.printSchema()
        # 默认显示前20条
        es_df.show()
        # 根据memberid分组，取每组中消费时间的最大值
        # F.current_date()：求当前的时间
        # F.from_unixtime("finishtime","yyyy-MM-dd")：把时间戳类型转换为yyyy-MM-dd的格式
        es_df = es_df.groupBy("memberid").agg(F.max("finishtime").alias("finishtime")) \
            .select("memberid", F.datediff(F.current_date(), F.from_unixtime("finishtime", "yyyy-MM-dd")).alias("days"))
        es_df.printSchema()
        es_df.show()
        # 对标签规则数据进行切割，转换为id，start，end三个字段--根据five_df的标签id，rule中的rule，即要根据这个5级标签规则打标签
        five_df = five_df.select("id", F.split("rule", "-")[0].alias("start"), F.split("rule", "-")[1].alias("end"))
        # 把业务数据es_df和标签规则数据进行join操作，取业务数据的memberid和标签规则中的id
        # 将所有类别的业务数据进行打标签以后--都改成统一的两列轻量级别的数据，这样不管你拿的目标数据表(业务数据)，打完标签以后，es中都只存在一张标签
        new_df = es_df.join(other=five_df, on=es_df["days"].between(five_df['start'], five_df['end']), how='left') \
            .select(es_df['memberid'].alias("userId"), five_df['id'].alias("tagsId"))
        five_df.printSchema()
        five_df.show()
        new_df.printSchema()
        new_df.show()
        return new_df


if __name__ == '__main__':
    # 表名:tbl_basic_tag
    # id=23  level=4
    consumerCycleModel = ConsumerCycleModel(23)
    consumerCycleModel.execute()

    # consumerCycleModel作为AbstractBaseModel类的子类，继承类以下方法
    # def execute(self):
    #     # # 0.初始化Spark环境
    #     self.__init__(self.fourTagId)
    #     # 1.根据4级标签ID，读取MySQL标签体系数据--读出来的结果是一张表
    #     input_df = self.read_mysql_data()
    #     # 2.过滤4级标签的数据，将四级标签的rule转换为esMeta对象，id不同，逻辑相同
    #     esMeta = self.input_df_to_esMeta(input_df) # input_df_to_esMeta方法里面会调用fourTagId并取出rule字段--
    #                                                                       只需要这个input_df的fourTagId=23这一行，取出rule字符串解析成操作的es目标表信息
    #     # 3.根据esMeta对象从ES中读取目标表信息--读出来的结果是一张表--一张待打匹配标签或者待统计标签或者待挖掘挖掘类标签的，指定字段数的表
    #     es_df = self.read_es_df_by_esMeta(esMeta) # 根据rule规则知道要操作的目标表

    #     # 4.根据4级标签ID，读取5级标签的数据
    #     # 刚刚我们只要input(来源与mysql)表中的rule字段来转换成meta对象去es中找待操作的数据表
    #     # 现在我们还需要input表中的其他字段值，level字段为5的值，这个值有很多，但我只要刚刚指定的rule字段下的5级值，取出的是规则值
    #     # 把规则值和刚刚找到的es表的数据进行操作(匹配、统计、挖掘)
    #     将pid=23,即父id=23（四级标签）的所有行找出来组成five_df的字段：id,rule的新dataframe--id是这个5级标签的id和这个5级标签的规则
    #     five_df = self.read_five_df_by_fourTagId(input_df) # five_df: DataFrame = input_df.where(f"pid = {self.fourTagId}").select("id", "rule")
    #     # 5.读出ES中的以前打好的标签表，将MySQL的5级标签id对这个原来的标签表进行打标签
    #
    #     new_df = self.compute(es_df, five_df) # 先通过compute打标签得到新的标签表new_df
    #
    #     try:
    #         # 6.从ES中读取历史用户标签数据
    #         old_df = self.read_old_df_from_es(esMeta)
    #         # 标签更新
    #         """
    #         需要传入改4级标签下的所有5级标签的ID号。不能直接给five_df（ID、rule）
    #         （1）通过five_df获取所有ID
    #         （2）传入所有ID（List）List不能直接传入到自定义函数中，自定义需要只能传入2种类型（Column、Str）
    #         （3）把list转换为字符串，使用固定的分割符号拼接
    #             ",".join(List)
    #         """
    #         fiveTagIDList = five_df.rdd.map(lambda x: x.id).collect()
    #
    #         # 7.将老的用户画像标签与新的标签进行合并，得到最终标签
    #         # 逐个遍历fiveTagIDList这个5级标签列表，逐个去遍历
    #         # merge_tags 是一个udf操作
    #         result_df = self.merge_old_df_and_new_df(new_df, old_df, F.lit(",".join(str(id) for id in fiveTagIDList)))
    #         # 上面的操作与 F.lit(",".join(fiveTagIDList)) 之间存在一些区别。主要区别在于 fiveTagIDList 的元素类型是否都是字符串类型
    #         # 所以，F.lit(",".join(str(id) for id in fiveTagIDList)) 是更通用和安全的做法，尤其是在不确定 fiveTagIDList 中元素类型时
    #     except:
    #         # 如果出了问题，说明这个任务是第一次运行，没有历史标签库
    #         # 可以使用new_df当做标签结果数据，避免合并出问题
    #         result_df = new_df
    #         print("--------------首次执行，跳过合并---------------")
    #     # 8.将最终的结果写入ES中
    #     self.write_result_df_to_es(result_df, esMeta)
    #     # 9.销毁Spark环境，释放资源
    #     self.close()
