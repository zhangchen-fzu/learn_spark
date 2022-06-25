from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]').appName('test').\
        config('spark.sql.shuffle.partitions', 2).getOrCreate()

    df = spark.read.format('csv').\
        option('header', True).option('sep', ';').\
        load(r'd:\pyspark\testdata\sql\people.csv')

    #对所有缺失值填充loss
    df.fillna('loss').show()
    #对job列的缺失值填充n/a
    df.fillna('N/A', subset=['job']).show()
    #对对应列填充对应值
    df.fillna({'name':'无名氏', 'age':0, 'job':'worker'}).show()