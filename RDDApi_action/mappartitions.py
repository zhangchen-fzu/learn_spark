from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("loacl[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,2,3,4])

    def process(iter):
        lst = []
        for it in iter:
            lst.append(it * 10)
        return lst

    res = rdd.mapPartitions(process)
    print(res.collect())