from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(["fw eqwe qee", "ewr rttr weq wer"])
    ##split分完之后是二维，要使用flatMap解除嵌套的功能，就变成一维了
    rdd1 = rdd.flatMap(lambda line:line.split(" "))
    print(rdd1.collect())