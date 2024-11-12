from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, sum, min, max, stddev, first, last

spark = SparkSession \
    .builder \
    .appName("first-app") \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()

data = [("Udhaya", "CSE", "Data Engineer", 10000),
        ("Siva", "IT", "Cloud Engineer", 7000),
        ("Giri", "CSBS", "Data Analyst", 9000),
        ("Kumar", "AIDS", "AI Engineer", 10000),
        ("Karthik", "CSE", "Developer", 11000),
        ("Pradeep", "IT", "Network Engineer", 6500)]

columns = ['Name', 'Department', 'Skill', 'Salary']

df = spark.createDataFrame(data, columns)

df_grouped = df.groupBy("Department").agg(
    avg("Salary").alias("Average_Salary"),
    count("Name").alias("Employee_Count"),
    sum("Salary").alias("Total_Salary"),
    min("Salary").alias("Min_Salary"),
    max("Salary").alias("Max_Salary"),
    stddev("Salary").alias("Salary_Std_Dev"),
    first("Name").alias("First_Employee"),
    last("Name").alias("Last_Employee")
)

df_grouped.show()

spark.stop()
