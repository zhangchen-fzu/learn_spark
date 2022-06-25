from pyspark import SparkConf, SparkContext
import jieba


# 分词、过滤、修正、组合
def split_fun(data):
    id = data[0]
    ques = data[1]
    lst = list(jieba.cut_for_search(ques))
    res = []
    for word in lst:
        if word not in ['谷', '帮', '客']:
            if word == '博学':
                word = '博学谷'
            elif word == '传智播':
                word = '传智播客'
            elif word == '院校':
                word = '院校帮'
            res.append((id + '_' + word, 1))
    return res


if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    file_rdd = sc.textFile(r"d:\pyspark\testdata\SogouQ.txt")
    content_rdd = file_rdd.map(lambda x: x.split('\t'))
    id_ques_rdd = content_rdd.map(lambda x: (x[1], x[2]))
    id_word_rdd = id_ques_rdd.flatMap(split_fun)
    res = id_word_rdd.reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False, numPartitions=1).take(5)
    print(res)
