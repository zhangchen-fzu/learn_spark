from pyspark import SparkConf, SparkContext
import json

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile(r'd:\pyspark\testdata\order.text')
    json_rdd = rdd.flatMap(lambda line:line.split("|"))
    ##通过json库将字符串组成的json数据转化为字典的形式
    dic_rdd = json_rdd.map(lambda line:json.loads(line))
    filt_rdd = dic_rdd.filter(lambda d:d['areaName'] == '北京')
    str_rdd = filt_rdd.map(lambda line:line['areaName'] + '_' + line['category'])
    res_rdd = str_rdd.distinct()
    print(res_rdd.collect())