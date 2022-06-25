from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 10), ('v', 3), ('d', 2), ('e', 1), ('g', 5), ('z', 5)])
    #根据key升序排序，一个分区保证整体有序
    rdd1 = rdd.sortBy(lambda x:x[0], ascending=True, numPartitions=1)
    print(rdd1.collect())