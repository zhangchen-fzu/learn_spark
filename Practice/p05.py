from pyspark.sql import SparkSession, functions as F
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import StringType

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]').appName('test').\
        config('spark.sql.shuffle.partitions', 2).getOrCreate()

    df = spark.read.format('json').load(r'd:\pyspark\testdata\mini.json').\
        dropna(thresh=1, subset=['storeProvince']).\
        filter("storeProvince != 'null'").\
        filter('receivable < 10000').\
        select('storeProvince', 'storeID', 'receivable', 'dateTS', 'payType')

    #TODO 1:各个省的销售额
    df1 = df.groupBy('storeProvince').sum('receivable').\
        withColumnRenamed('sum(receivable)', 'money').\
        withColumn('money', F.round('money', 2)).\
        orderBy('money', ascending=False)
    df1.show(truncate=False)

    #TODO 2:top3销售省份中，有多少店铺达到日均销售额1000+
    top3_province_df = df1.limit(3).select('storeProvince').withColumnRenamed('storeProvince', 'top3Province')
    top3_province_join_df = df.join(top3_province_df, on=df['storeProvince'] == top3_province_df['top3Province'])
    top3_province_join_df.persist(StorageLevel.MEMORY_AND_DISK)

    df2 = top3_province_join_df.groupBy('storeProvince', 'storeID',
                                  F.from_unixtime(df['dateTS'].substr(0, 10), 'yyyy-MM-dd').alias('day')).\
        sum('receivable').withColumnRenamed('sum(receivable)', 'money').\
        filter('money > 1000').\
        dropDuplicates(subset=['storeID']).\
        groupBy('storeProvince').count()
    df2.show()

    #TODO 3:top3销售省份中，各个省份的平均订单单价
    df3 = top3_province_join_df.groupBy('storeProvince').avg('receivable').\
        withColumnRenamed('avg(receivable)', 'money').\
        withColumn('money', F.round('money', 2)).\
        orderBy('money', ascending=False)
    df3.show()

    # TODO 4:top3销售省份中，各个省份的支付工具的支付比例
    top3_province_join_df.createTempView('top3_province')

    def process(p):
        return str(round(p * 100, 2)) + '%'
    my_udf = F.udf(process, StringType())

    df4 = spark.sql('''
                select storeProvince, payType,
                round(count(*) / total, 2) as p 
                from 
                    (
                        select storeProvince, payType, count(*) over(partition by storeProvince) as total
                        from top3_province
                    ) as a
                group by storeProvince, payType, total
            ''').withColumn('p', my_udf('p'))
    df4.show()


