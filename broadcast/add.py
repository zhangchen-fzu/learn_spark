from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)

    #count是本地的，与excutor是分开的，excutor执行+1并不会对本地结果产生影响
    # count = 0
    # def add_fun(data):
    #     global count
    #     count += 1
    #     print(count)
    #
    # rdd2 = rdd.map(add_fun)
    # rdd2.collect()
    # print(count)

    ##将count定义为累加器，值的变化与excutor结果同步
    count = sc.accumulator(0)
    def add_fun(data):
        global count
        count += 1
        print(count)

    rdd2 = rdd.map(add_fun)
    rdd2.cache()
    rdd2.collect()

    #由于执行完rdd2.collect()之后，rdd2会消失。因为没有缓存起来，因此再次调用时，从头开始计算，累加器count在原来的基础上又重新加上了10
    rdd3 = rdd2.map(lambda x:x)
    rdd3.collect()
    print(count)
