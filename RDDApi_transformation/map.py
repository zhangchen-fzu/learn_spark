from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4])


    def multiply(val):
        return val * 10


    ##两种写法都可以，使用匿名函数lambda更适合只有一行的函数体
    print(rdd.map(multiply).collect())
    print(rdd.map(lambda val: val * 10).collect())
