from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, ArrayType, StringType

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]').appName('test'). \
        config('spark.sql.shuffle.partitions', 2).getOrCreate()
    sc = spark.sparkContext

    rdd = sc.parallelize([['aa vv dd'], ['erw gre ewq qwe']])
    df = rdd.toDF(['line'])

    def split_fuc(line):
        return line.split(' ')

    #TODO 1: sparksession.udf.register构建udf
    udf2 = spark.udf.register('udf1', split_fuc, ArrayType(StringType()))
    #sql风格代码1
    df.selectExpr('udf1(line)').show()
    #sql风格代码2
    df.createTempView('lines')
    spark.sql('select udf1(line) from lines').show(truncate=False)
    #dsl风格代码
    df.select(udf2(df['line'])).show(truncate=False)

    #TODO 2: functions.udf构建udf
    udf3 = F.udf(split_fuc, ArrayType(IntegerType()))
    df.select(udf3(df['line'])).show(truncate=False)

