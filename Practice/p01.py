from pyspark import SparkConf, SparkContext
import jieba


# 分词、过滤、修正
def jieba_cut(data):
    res = []
    lst = list(jieba.cut_for_search(data))
    for word in lst:
        if word not in ['谷', '帮', '客']:
            if word == '博学':
                word = '博学谷'
            elif word == '传智播':
                word = '传智播客'
            elif word == '院校':
                word = '院校帮'
            res.append((word, 1))
    return res


if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    file_rdd = sc.textFile(r"d:\pyspark\testdata\SogouQ.txt")
    context_rdd = file_rdd.map(lambda x: x.split("\t"))
    query_rdd = context_rdd.map(lambda x: x[2])
    words_rdd = query_rdd.flatMap(jieba_cut)
    res = words_rdd.reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False, numPartitions=1).take(5)
    print(res)
