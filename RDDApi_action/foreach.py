from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([9, 1, 5, 4, 7, 8, 9, 5, 6])

    ##foreach()方法没有返回值
    print(rdd.foreach(lambda x: x * 10))
    rdd.foreach(lambda x: print(x * 10))