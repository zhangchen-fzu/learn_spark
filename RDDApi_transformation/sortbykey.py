from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('C', 1), ('a', 2), ('A', 3), ('b', 4)])
    ##排序函数指的是对key进行一定的处理。处理仅仅是在排序过程中，并不会对结果集产生影响
    rdd1 = rdd.sortByKey(ascending=True, numPartitions=1, keyfunc=lambda key:key.lower())
    print(rdd1.collect())
