from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("loacl[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile(r"d:\pyspark\testdata\words.txt")
    rdd1 = rdd.flatMap(lambda line: line.split(" "))
    rdd2 = rdd1.map(lambda x: (x, 1))

    ##使用cache()缓存到内存中
    rdd2.cache()
    ##使用persist()缓存到内存中
    rdd2.persist(StorageLevel.MEMORY_ONLY)

    rdd3 = rdd2.reduceByKey(lambda a, b: a + b)
    print(rdd3.collect())

    rdd3 = rdd2.groupByKey()
    rdd4 = rdd3.mapValues(lambda x:sum(x))
    print(rdd4.collect())

    ##清理缓存
    rdd2.unpersist()