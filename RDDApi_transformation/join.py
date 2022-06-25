from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([(1001, "zhangsan"), (1002, "lisi"), (1003, "wangwu"), (1004, "zhaoliu")])
    rdd2 = sc.parallelize([(1001, "科技部"), (1002, "销售部")])

    #内连接。几种连接都按照key进行连接
    print(rdd1.join(rdd2).collect())
    #左外连接
    print(rdd1.leftOuterJoin(rdd2).collect())
    #右外连接
    print(rdd1.rightOuterJoin(rdd2).collect())