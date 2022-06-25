from pyspark import SparkConf, SparkContext


if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    stu_info_list = [
        (1, '张大仙', 11),
        (2, '王晓晓', 13),
        (3, '张甜甜', 11)
    ]
    ##将stu_info_list封装进broadcast中
    broadcast = sc.broadcast(stu_info_list)
    score_info_rdd = sc.parallelize([
        (1, '语文', 99),
        (2, '数学', 99),
        (3, '英语', 99),
        (1, '数学', 99),
        (2, '编程', 99),
        (3, '语文', 99)
    ])
    def join_fun(data):
        id = data[0]
        name = ''
        ##使用broadcast中的值
        for stu_info in broadcast.value:
            if stu_info[0] == id:
                name = stu_info[1]
        return (name, data[1], data[2])
    print(score_info_rdd.map(join_fun).collect())



