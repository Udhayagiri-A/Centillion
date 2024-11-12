from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession \
    .builder \
    .appName("first-app") \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()

data = [("Udhaya", "CSE", "Data Engineer", 10000),
        ("Siva", "IT", "Cloud Engineer", 7000),
        ("Pradeep", "IT", "Network Engineer", 6500)]

columns = ['Name', 'Department', 'Skill', 'Salary']

df = spark.createDataFrame(data, columns)

df_copy = df.withColumnRenamed("Name", "Employee_Name") \
            .withColumnRenamed("Skill", "Employee_Skill") \
            .withColumnRenamed("Salary", "Employee_Salary")

df_joined = df.join(df_copy, on="Department", how="inner")

df_joined.select(
    "Department", 
    "Name", 
    "Employee_Name", 
    "Employee_Skill", 
    "Employee_Salary", 
    "Salary"
).show()

spark.stop()
