from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank, dense_rank

spark = SparkSession.builder.appName("RankingExample").getOrCreate()

data = [
    (1, 'Sam', 101, 50000, 'A'),
    (2, 'Raj', 101, 50000, 'B'),
    (3, 'Tom', 101, 20000, 'B'),
    (4, 'Danny', 202, 30000, 'C'),
    (5, 'Paul', 202, 20000, 'C'),
    (6, 'Siri', 202, 80000, 'D'),
    (7, 'Rajesh', 303, 50000, 'E'),
    (8, 'Donald', 303, 25000, 'D'),
    (9, 'John', 303, 50000, 'A'),
    (10, 'Joe', 303, 90000, 'E')
]

columns = ["en", "name", "dept", "Salary", "City"]
df = spark.createDataFrame(data, columns)

window_spec = Window.partitionBy("dept").orderBy(col("Salary").desc())
df = df.withColumn("row_number", row_number().over(window_spec)) \
       .withColumn("rank", rank().over(window_spec)) \
       .withColumn("dense_rank", dense_rank().over(window_spec))

df.show()
