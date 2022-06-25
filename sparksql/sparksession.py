from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

    ##使用rdd的形式，直接调用sparkContext
    sc = spark.sparkContext

    ##读取数据
    df = spark.read.csv(r'd:\pyspark\testdata\stu_score.txt')
    df2 = df.toDF('id', 'name', 'score')
    #打印结构
    df2.printSchema()
    #打印数据
    df2.show()
    #创建视图score
    df2.createTempView('score')

    # 使用sql读取数据
    spark.sql('select * from score where name="语文" limit 5').show()

    #使用dsl风格
    df2.where('name="语文"').limit(5).show()

