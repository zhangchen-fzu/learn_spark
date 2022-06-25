from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([9, 1, 5, 4, 7, 8, 9, 5, 6])

    #默认升序排序
    print(rdd.takeOrdered(3))
    #加入函数，使之按照降序排序
    print(rdd.takeOrdered(3, lambda x: -x))
