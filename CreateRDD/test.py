from pyspark import SparkConf, SparkContext


if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("WordCountHelloWorld")
    # 通过SparkConf对象构建SparkContext对象
    sc = SparkContext(conf=conf)

    # 读取文件
    file_rdd = sc.textFile(r"d:\pyspark\testdata\words.txt")

    # 将单词进行切分
    words_rdd = file_rdd.flatMap(lambda line: line.split(" "))

    # 将单词转换为元组对象，key是单词，value是数字1
    words_with_one_rdd = words_rdd.map(lambda x: (x, 1))

    # 将元组的value 按照key来分组，对所有value进行聚合操作（相加）
    result_rdd = words_with_one_rdd.reduceByKey(lambda a, b: a + b)

    # 通过collect方法收集rdd的数据，打印输出结果
    print(result_rdd.collect())
