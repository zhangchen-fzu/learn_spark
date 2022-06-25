


from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    ##创建SparkContext对象
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    ##通过parallelize并行化创建rdd，即将本地的list转化为rdd。parallelize不写第二个分区参数，则会根据cpu的情况给予一定的分区数量
    rdd = sc.parallelize([1,2,3], 3)
    ##通过getNumPartitions显示现在rdd的分区数量
    print(rdd.getNumPartitions())
    ##通过collect方法将rdd转化为list的形式输出
    print(rdd.collect())
