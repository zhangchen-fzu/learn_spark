from pyspark.sql import SparkSession, functions as F


if __name__ == '__main__':

    spark = SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    sc = spark.sparkContext

    ##SQL风格的wordcount
    rdd = sc.textFile(r'd:\pyspark\testdata\words.txt')
    rdd2 = rdd.flatMap(lambda x:x.split(' ')).map(lambda x:[x])
    df = rdd2.toDF(['word'])
    df.createTempView('words')
    spark.sql('select word, count(*) as cnt from words group by word order by cnt desc').show()

    ##DSL风格的wordcount
    df = spark.read.format('text').load(r'd:\pyspark\testdata\words.txt')
    df2 = df.withColumn('value', F.explode(F.split(df['value'], ' ')))
    df2.groupBy('value').\
        count().\
        withColumnRenamed('value', 'word').\
        withColumnRenamed('count', 'cnt').\
        orderBy('cnt', ascending=False).\
        show()


