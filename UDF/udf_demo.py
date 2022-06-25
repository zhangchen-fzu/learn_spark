from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType

if __name__ == '__main__':

    spark = SparkSession.builder.master('local[*]').appName('test').\
        config('spark.sql.shuffle.partitions', 2).getOrCreate()
    sc= spark.sparkContext

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6]).map(lambda x:[x])
    df = rdd.toDF(['numbers'])

    #TODO 1: 使用sparksession.udf.register()构建UDF。后续可以使用SQL及DSL的代码风格
    def num_ride_10(num):
        return num * 10
    #参数1：用于sql风格代码中，表示表的名称；参数2：udf函数；参数3：udf的返回值类型
    udf2 = spark.udf.register('udf1', num_ride_10, IntegerType())
    #SQL风格代码，selectExpr表示select表达式
    df.selectExpr('udf1(numbers)').show()
    #DSL风格代码，使用udf对象，参数为column对象
    df.select(udf2(df['numbers'])).show()

    #TODO 2:使用functions.udf构建udf。后续仅可以用DSL代码风格
    udf3 = F.udf(num_ride_10, IntegerType())
    df.select(udf3(df['numbers'])).show()


