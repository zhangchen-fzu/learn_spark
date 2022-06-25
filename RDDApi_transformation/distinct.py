from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 1, 1, 3, 3, 4, 5, 6])
    #无需参数
    res = rdd.distinct()
    print(res.collect())
