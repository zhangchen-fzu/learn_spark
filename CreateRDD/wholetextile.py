from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    ##读取很多小文件构成的小文件夹
    rdd = sc.wholeTextFiles(r'd:\pyspark\testdata\tiny_files')
    print(rdd.map(lambda x:x[1]).collect())