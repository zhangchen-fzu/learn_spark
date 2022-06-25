from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 2), ('b', 2), ('c', 4), ('b', 1)])
    #按照元组的第一个位置进行分组，也就是按照key进行分组
    res = rdd.groupBy(lambda x:x[0])
    print(res.collect()) #[('b', <pyspark.resultiterable.ResultIterable object at 0x00000296EF6B0288>), ('c', <pyspark.resultiterable.ResultIterable object at 0x00000296EF6AA688>), ('a', <pyspark.resultiterable.ResultIterable object at 0x00000296EF713388>)]
    #分组之后value返回的是一个可迭代的对象，可通过list将其强制转换一下
    print(res.map(lambda x:(x[0], list(x[1]))).collect())  #[('b', [('b', 2), ('b', 1)]), ('c', [('c', 4)]), ('a', [('a', 1), ('a', 2)])]