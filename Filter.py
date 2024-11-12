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
        ("Giri", "CSBS", "Data Analyst", 9000),
        ("Kumar", "AIDS", "AI Engineer", 10000),
        ("Karthik", "CSE", "Developer", 11000),
        ("Pradeep", "IT", "Network Engineer", 6500)]

columns = ['Name', 'Department', 'Skill', 'Salary']

df = spark.createDataFrame(data, columns)

df_filtered = df.filter(col("Salary") > 7000)

df_filtered.show()

spark.stop()
