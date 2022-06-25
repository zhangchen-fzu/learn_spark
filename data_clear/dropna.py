from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]').appName('test').\
        config('spark.sql.shuffle.partitions', 2).getOrCreate()

    df = spark.read.format('csv').\
        option('header', True).option('sep', ';').\
        load(r'd:\pyspark\testdata\sql\people.csv')

    #默认any，即样本中存在null，就将该样本删除
    df.dropna().show()
    #样本中的所有值都为null，才删除该样本
    df.dropna('all').show()
    #样本有3列有值，就留下。否则删除该样本
    df.dropna(thresh=3).show()
    #'name', 'age'两列有1个有值，则留下，否则删除
    df.dropna(thresh=1, subset=['name', 'age']).show()




