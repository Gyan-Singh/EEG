import time
import pandas as pd
from pyspark.sql import SparkSession

# Function to compute the average of a column using Pandas
def calculate_average_with_pandas(file_path, column_name):
    start_time = time.time()
    data = pd.read_csv(file_path)
    print(data.head())
    average = data[column_name].mean()
    end_time = time.time()
    return average, end_time - start_time

# Function to compute the average of a column using Spark
def calculate_average_with_spark(file_path, column_name):
    spark = SparkSession.builder.appName("average_comparison").getOrCreate()
    start_time = time.time()
    data = spark.read.csv(file_path, header=True, inferSchema=True)
    average = data.select(column_name).agg({column_name: 'avg'}).collect()[0][0]
    end_time = time.time()
    spark.stop()
    return average, end_time - start_time

# Path to the "california_housing_test" dataset (replace with your dataset's path)
file_path = "C:/Users/gyans/Downloads/mnist_test.csv"

# Column for which to calculate the average
column_name = '7'

# Calculate average and measure time using Pandas
pandas_average, pandas_time = calculate_average_with_pandas(file_path, column_name)
# Print the results and time taken by each method
print(f"Average using Pandas: {pandas_average}")
print(f"Time taken to compute average using Pandas: {pandas_time} seconds")

# Calculate average and measure time using Spark
# spark_average, spark_time = calculate_average_with_spark(file_path, column_name)


print(f"Average using Spark: {spark_average}")
print(f"Time taken to compute average using Spark: {spark_time} seconds")


