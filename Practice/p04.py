from pyspark import SparkConf, SparkContext
from operator import add
from pyspark.storagelevel import StorageLevel


if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    file_rdd = sc.textFile(r"d:\pyspark\testdata\apache.log")
    content_rdd = file_rdd.map(lambda x: x.split(' '))
    # content_rdd.persist(StorageLevel.MEMORY_ONLY)

    ##pv
    # url_rdd = content_rdd.map(lambda x:(x[4], 1))
    # res1 = url_rdd.reduceByKey(add).collect()
    # print("PV:", res1)

    ##uv
    # userid_rdd = content_rdd.map(lambda x:(x[1], 1))
    # res2 = userid_rdd.reduceByKey(add).collect()
    # print("uv", res2)

    ##ip
    # ip_rdd = content_rdd.map(lambda x: x[0])
    # res3 = ip_rdd.distinct().collect()
    # print("ip:", res3)

    ##top_url
    url_rdd = content_rdd.map(lambda x:(x[4], 1))
    res4 = url_rdd.reduceByKey(add).sortBy(lambda x:x[1], ascending=False, numPartitions=1).take(1)
    print("top1_url:", res4)





