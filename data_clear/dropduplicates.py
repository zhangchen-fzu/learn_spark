from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.\
        master('local[*]').appName('test').\
        config('spark.sql.shuffle.partitions', 2).\
        getOrCreate()

    df = spark.read.format('csv').option('header', True).option('sep', ';').\
        load(r'd:\pyspark\testdata\sql\people.csv')

    ##全局去重
    df.dropDuplicates().show()
    ##去除指定列的重复
    df.dropDuplicates(['age', 'job']).show()