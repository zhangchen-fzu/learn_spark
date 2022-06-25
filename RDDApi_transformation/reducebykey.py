from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 2), ('a', 4)])
    ##reduceByKey中的函数体接受两个参数，返回一个同类型的参数
    rdd1 = rdd.reduceByKey(lambda a, b: a + b)
    print(rdd1.collect())
