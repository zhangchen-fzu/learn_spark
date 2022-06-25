from pyspark.sql import SparkSession


if __name__ == '__main__':

    spark = SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    sc = spark.sparkContext

    ##读取json数据，无需构建数据的结构，因为json数据内部有列名，且能通过引号识别数据的类型
    df = spark.read.format('json').load(r'd:\pyspark\testdata\sql\people.json')
    df.printSchema()
    df.show()