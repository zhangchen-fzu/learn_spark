import pandas as pd
from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    sc = spark.sparkContext

    ##构建pandas数据
    pdf = pd.DataFrame(
        {
            'name':['zhangsan', 'lisi'],
            'age':[11, 12]
        }
    )

    #将pandas转化为分布式的pandas
    df = spark.createDataFrame(pdf)
    df.printSchema()
    df.show()
