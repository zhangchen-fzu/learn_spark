from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 22), ('c', 30)])
    #使用map对元组的value进行处理
    rdd1 = rdd.map(lambda x: (x[0], x[1] * 10))
    print(rdd1.collect())

    #使用mapValues对元组的value进行处理
    rdd2 = rdd.mapValues(lambda value: value * 10)
    print(rdd2.collect())
