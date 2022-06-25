from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([1, 2, 3, 1])
    rdd2 = sc.parallelize([7, 5, 4, 6])
    rdd3 = rdd1.intersection(rdd2)
    print(rdd3.collect()) #[1, 3]
