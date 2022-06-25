from pyspark import SparkConf, SparkContext
from operator import add

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    file_rdd = sc.textFile(r"d:\pyspark\testdata\SogouQ.txt")
    content_rdd = file_rdd.map(lambda x: x.split('\t'))
    time_rdd = content_rdd.map(lambda x: x[0])
    hour_rdd = time_rdd.map(lambda x: (x.split(":")[0], 1))
    res = hour_rdd.reduceByKey(add).sortBy(lambda x: x[1], ascending=False, numPartitions=1).take(5)
    print(res)
