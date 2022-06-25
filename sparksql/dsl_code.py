from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    sc = spark.sparkContext

    df = spark.read.format('csv').schema('id INT, subject STRING, score INT').load(r'd:\pyspark\testdata\sql\stu_score.txt')

    ##dsl风格编码==============================
    #select语句
    df.select('id', 'subject').show()
    df.select(['id', 'subject']).show()
    id_column = df['id']
    subject_column = df['subject']
    df.select(id_column, subject_column).show()
    df.select([id_column, subject_column]).show()

    ##filter 及 where 语句
    df.where('score < 99').show()
    df.where(df['score'] < 99).show()
    df.filter('score < 99').show()
    df.filter(df['score'] < 99).show()

    ##group by 语句
    ##group by 语句的返回值不是df类型的，而是groupeddata类型的
    ##通过聚合函数聚合之后sum|count|min|max|avg等，又可以转换为df类型的。groupeddata类型仅仅是一个中转对象
    df.groupBy('subject').count().show()
    df.groupBy(df['subject']).count().show()

