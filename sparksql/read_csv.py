from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    sc = spark.sparkContext

    ##通过option中选项指定csv的格式，通过在schema中以字符串的形式指定列名及类型
    df = spark.read.format('csv').\
        option('sep', ';').\
        option('header', True).\
        option('encoding', 'utf-8').\
        schema("name STRING, age INT, job STRING").\
        load(r'd:\pyspark\testdata\sql\people.csv')
    df.printSchema()
    df.show()
