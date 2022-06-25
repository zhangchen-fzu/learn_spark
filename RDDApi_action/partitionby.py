from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("loacl[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a',1), ('b',2), ('b',1), ('c',3)])

    def process(key):
        if key in ['a','b']:
            return 0
        return 1
    
    #自定义分区函数
    rdd1 = rdd.partitionBy(2, process)
    rdd2 = rdd1.glom()
    print(rdd2.collect())
