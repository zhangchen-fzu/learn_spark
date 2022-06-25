from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("loacl[*]")
    sc = SparkContext(conf=conf)

    ##告知spark开启checkpoint功能
    sc.setCheckpointDir(r'hdfs路径')

    rdd = sc.textFile(r"d:\pyspark\testdata\words.txt")
    rdd1 = rdd.flatMap(lambda line: line.split(" "))
    rdd2 = rdd1.map(lambda x: (x, 1))

    ##调用checkpoint API，保存数据
    rdd2.checkpoint()

    rdd3 = rdd2.reduceByKey(lambda a, b: a + b)
    print(rdd3.collect())

    rdd3 = rdd2.groupByKey()
    rdd4 = rdd3.mapValues(lambda x:sum(x))
    print(rdd4.collect())
