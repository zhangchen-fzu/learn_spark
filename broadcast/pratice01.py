from pyspark import SparkConf, SparkContext
import re

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster('local[*]')
    sc = SparkContext(conf=conf)

    file_rdd = sc.textFile(r'd:\pyspark\testdata\accumulator_broadcast_data.txt')
    unblank_line_rdd = file_rdd.filter(lambda x: x.strip())
    strs_rdd = unblank_line_rdd.map(lambda x: x.strip())
    words_rdd = strs_rdd.flatMap(lambda x: re.split('\s+', x))

    unnormal_char = [',', '.', '!', '#', '%', '$']
    broadcast = sc.broadcast(unnormal_char)
    count = sc.accumulator(0)

    def filter_func(data):
        global count
        if data in broadcast.value:
            count += 1
            return False
        return True

    normword_rdd = words_rdd.filter(filter_func)
    res_rdd = normword_rdd.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)

    print("正常单词计数：", res_rdd.collect())
    print("非正常字符的个数：", count)

