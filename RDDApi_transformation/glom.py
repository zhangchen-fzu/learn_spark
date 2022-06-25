from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 4)
    # 查看分区
    rdd1 = rdd.glom()
    print(rdd1.collect())
    # 解除嵌套
    print(rdd1.flatMap(lambda x: x).collect())
