from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("loacl[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9],3)
    ##一般不对分区做加操作
    print(rdd.repartition(5).getNumPartitions())
    ##会对分区做减操作
    print(rdd.repartition(1).getNumPartitions())
    ##没有设置shuffle参数的coalesce()不起作用
    print(rdd.coalesce(5).getNumPartitions())
    print(rdd.coalesce(1, shuffle=True).getNumPartitions())