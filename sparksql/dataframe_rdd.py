from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.appName("test").master('local[*]').getOrCreate()
    sc = spark.sparkContext

    rdd = sc.textFile(r'd:\pyspark\testdata\sql\people.txt')
    rdd1 = rdd.map(lambda x:x.split(',')).map(lambda x:(x[0], int(x[1])))

    ##通过RDD的形式创建dataframe，无法指定列的数据类型以及列是否为空
    df = spark.createDataFrame(rdd1, schema=['name', 'age'])
    df.printSchema()
    #参数1表示展示多少数据，默认前20条；参数2表示是否对列进行截断，如果设置为True，表示对列进行截断，超过20的以...代替
    df.show(20, False)

    #创建临时表
    df.createOrReplaceTempView('people')
    spark.sql('select * from people where age < 30').show()