from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile(r'D:\pyspark\testdata\words.txt')
    rdd1 = rdd.flatMap(lambda line:line.split(" "))
    rdd2 = rdd1.map(lambda x:(x, 1))
    ##对kv型的数据统计key出现的次数
    res = rdd2.countByKey()
    print(res)
    print(type(res))