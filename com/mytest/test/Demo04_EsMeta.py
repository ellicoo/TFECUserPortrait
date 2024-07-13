# 说人话：将前端的规则要求--字符串 给拆出我们需要操作的ES的哪个数据库的类型以及筛选出哪些字段--将切词出来的结果变量打包成一个类型变量即对象，
# 即将字典变量遍历类变量，由类变量返回一个我们需要的操作对象信息

# 封装的EsMeta类
class EsMeta(object):
    inType: str = None
    esNodes: str = None
    esType: str = None
    esIndex: str = None
    selectFields: str = None

    def __init__(self, inType, esNodes, esType, esIndex, selectFields):
        self.inType = inType
        self.esNodes = esNodes
        self.esType = esType
        self.esIndex = esIndex
        self.selectFields = selectFields

    def __str__(self):
        return f'{self.inType},{self.esNodes},{self.esType},{self.esIndex},{self.selectFields}'


# 需求：把如下字符串的标签规则解析成一个EsMeta对象
tagRule = 'inType=Elasticsearch##esNodes=up01:9200##esIndex=tfec_tbl_users##esType=_doc##selectFields=id,birthday'


def tagRuleStrToEsMeta(tagRule: str):
    # 根据##号切割
    tagRuleList = tagRule.split("##")
    # 定义一个字典
    kvDict = {}
    # 迭代出每一个属性信息
    for tags in tagRuleList:
        # tags:inType=Elasticsearch，根据=号切割
        kvTags = tags.split("=")
        # key=kvTags[0]
        # value=kvTags[1]
        kvDict[kvTags[0]] = kvTags[1]
    return kvDict


if __name__ == '__main__':
    # 根据标签规则，把标签规则解析成EsMeta对象
    kvDict = tagRuleStrToEsMeta(tagRule)
    print(f"kvDict的内容为：{kvDict}")
    # **：可变参数，可以把对象中的参数一个个地对应上，类似于写下面注释的代码
    # 调用时的**表示展开字典的键值对，然后传入参数
    esMeta = EsMeta(**kvDict)
    # esMeta = EsMeta(inType=kvDict['inType'],
    #                 esNodes=kvDict['esNodes'],
    #                 esType=kvDict['esType'],
    #                 esIndex=kvDict['esIndex'],
    #                 selectFields=kvDict['selectFields'])
    print(f"kvDict的内容为：{esMeta}")
