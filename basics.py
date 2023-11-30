from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# Creating spark session
spark = SparkSession.builder.appName('Basics').getOrCreate()

# Defining schema for Dataframe
data_schema = [StructField('Region', StringType(), True),
				StructField('Exchange', StringType(), True),
				StructField('Index', StringType(), True),
				StructField('Currency', StringType(), True)]

final_struct = StructType(data_schema)

# Reading CSV file
df = spark.read.option("header", True).csv('../data/stock-data/indexInfo.csv', schema=final_struct)

# prints first 20 rows
df.show()

# prints Dataframe schema
df.printSchema()

# print column names
df.columns

# prints columns with datatypes
df.describe()

# prints statistical summary
df.describe().show()


