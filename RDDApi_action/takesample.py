from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([9, 1, 5, 4, 7, 8, 9, 5, 6])

    ##有放回的抽取23个元素出来
    print(rdd.takeSample(True, 23, 1))
    ##无放回的抽取，抽取的元素个数大于原有个数的时候，回取出元素的所有值出来
    print(rdd.takeSample(False, 55, 1))