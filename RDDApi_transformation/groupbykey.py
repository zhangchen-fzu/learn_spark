from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 2), ('b', 2), ('b', 3), ('b', 4), ('c', 5)])
    rdd1 = rdd.reduceByKey(lambda a, b: a + b)
    print(rdd1.collect())

    rdd2 = rdd.groupByKey()
    print(rdd2.map(lambda x:(x[0], list(x[1]))).collect())
