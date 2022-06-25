from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    ##textFile读取文件的形式构建rdd
    rdd = sc.textFile(r"d:\pyspark\testdata\words.txt", 100)
    print(rdd.getNumPartitions()) #分区数参数指的是最小的分区数，不一定有效
    print(rdd.collect())

 