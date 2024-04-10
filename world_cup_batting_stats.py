from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DecimalType

# Creating spark session
spark = SparkSession.builder.appName('World Cup Matches Batting Stats').getOrCreate()

# Defining schema for Dataframe
data_schema = [
		StructField('match_no', IntegerType(), True),
		StructField('match_between', StringType(), True),
		StructField('team_innings', StringType(), True),
		StructField('batsman_name', StringType(), True),
		StructField('batting_position', StringType(), True),
		StructField('dismissal', StringType(), True),
		StructField('runs', IntegerType(), True),
		StructField('balls', IntegerType(), True),
		StructField('4s', IntegerType(), True),
		StructField('6s', IntegerType(), True),
		StructField('strike_rate', DecimalType(), True),
	]

final_struct = StructType(data_schema)

# Reading CSV file
df = spark.read.option("header", True).csv('../data/batting_summary.csv', schema=final_struct)

print("First 5 Rows from dataframe: \n")
df.show(5)

print("Dataframe schema: \n")
df.printSchema()

# print column names
print("Dataframe columns: \n")
df.columns()

# prints statistical summary
# df.describe().show()


