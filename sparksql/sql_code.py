from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    sc = spark.sparkContext

    df = spark.read.format('csv').schema('id INT, subject STRING, score INT').\
        load(r'd:\pyspark\testdata\sql\stu_score.txt')

    ##SQL风格的代码================
    ##创建临时表
    df.createTempView('score') ##普通临时表
    df.createOrReplaceTempView('score_1') ##如果有这个表则用新创建的这个临时表来替换
    df.createGlobalTempView('score_2') ##创建全局临时表，共多个sparksession使用

    ##使用临时表
    spark.sql('select subject, count(*) as nums from score group by subject').show()
    spark.sql('select subject, count(*) as nums from score_1 group by subject').show()
    spark.sql('select subject, count(*) as nums from global_temp.score_2 group by subject').show()