from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([9, 1, 5, 4, 7, 8, 9, 5, 6])

    print(rdd.first())
    print(rdd.take(5))
    print(rdd.top(3))
    print(rdd.count())