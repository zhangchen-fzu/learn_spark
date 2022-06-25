from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':

    spark = SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    sc = spark.sparkContext

    rdd = sc.textFile(r'd:\pyspark\testdata\sql\people.txt')
    rdd1 = rdd.map(lambda x:x.split(',')).map(lambda x:(x[0], int(x[1])))

    ##同第一种方法一样，无法指定列的数据类型，以及列是否可以为空
    df1 = rdd1.toDF(['name', 'age'])
    df1.printSchema()
    df1.show()

    ##同第二种方法一样，可以指定列的数据类型，以及列是否为空
    scheam = StructType().add('name', StringType(), True).add('age', IntegerType(), False)
    df2 = rdd1.toDF(schema=scheam)
    df2.printSchema()
    df2.show()