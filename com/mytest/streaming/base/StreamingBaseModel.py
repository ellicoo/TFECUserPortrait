from pyspark.sql import SparkSession, DataFrame
import os

"""
-------------------------------------------------
   Description :	TODO：流式任务基类
   SourceFile  :	StreamingBaseModel
   Author      :	mytest team
-------------------------------------------------
"""

# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/envs/pyspark_env/bin/python'

class StreamingBaseModel:
    ###############1.给每一部定义一个方法###############
    #todo 0.初始化环境
    def __init__(self,master,appName,numPartitions,kafkaServers,offsets,topic,database_name,table_name,outputMode):
        self.__spark = SparkSession \
            .builder \
            .master(master) \
            .appName(appName) \
            .config("spark.sql.shuffle.partitions", numPartitions) \
            .getOrCreate()
        self.kafkaServers = kafkaServers
        self.offsets = offsets
        self.topic = topic
        self.database_name = database_name
        self.table_name = table_name
        self.outputMode = outputMode

    #todo 1.读取Kafka的数据
    def read_from_kafka(self):
        input_df = self.__spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafkaServers) \
            .option("startingOffsets", self.offsets) \
            .option("subscribe", self.topic) \
            .load()
        return input_df

    #todo 2.对数据做ETL操作
    def etl_data(self,input_df):
        pass

    #todo 3.指标统计分析
    def compute(self,input_df):
        pass

    #todo 4.保存结果
    def save_result(self,input_df):
        # 采用批量写入到MySQL的方式
        def saveToMySQL(batch_df: DataFrame, batch_id):
            batch_df.write.jdbc(url=f'jdbc:mysql://up01:3306/{self.database_name}',
                                table=self.table_name,
                                mode='overwrite',
                                properties={"user": "root", "password": "123456"})

        # 写出结果到MySQL中
        input_df.writeStream.outputMode(self.outputMode).foreachBatch(saveToMySQL).start()

        # 结果输出到console
        input_df.writeStream.format("console").outputMode(self.outputMode).start().awaitTermination()


    ###############2.组装上述的方法###############
    def execute(self):
        #1.读取Kafka的数据
        input_df = self.read_from_kafka()
        #2.对数据做ETL操作
        input_df = self.etl_data(input_df)
        #3.指标统计分析
        input_df = self.compute(input_df)
        #4.保存结果
        self.save_result(input_df)
