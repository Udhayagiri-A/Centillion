from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from pyspark.sql.functions import sum as spark_sum, avg, count, desc, row_number, to_date
from pyspark.sql.window import Window

spark = SparkSession.builder.appName(EcommerceAnalysis).getOrCreate()

schema = StructType([
    StructField(transaction_id, StringType(), True),
    StructField(user_id, StringType(), True),
    StructField(product_id, StringType(), True),
    StructField(category, StringType(), True),
    StructField(amount, DoubleType(), True),
    StructField(transaction_date, StringType(), True)
])

data = [
    (1, 1, 1, Electronics, 1000.0, 2024-11-01),
    (2, 1, 2, Books, 550.0, 2024-11-02),
    (3, 1, 3, Electronics, 150.0, 2024-11-05),
    (4, 2, 4, Appliances, 20000.0, 2024-11-03),
    (5, 2, 5, Appliances, 2500.0, 2024-11-04),
    (6, 3, 6, Sports, 800.0, 2024-11-01),
    (7, 3, 7, Sports, 650.0, 2024-11-02),
    (8, 3, 8, Books, 400.0, 2024-11-03)
]

df = spark.createDataFrame(data, schema=schema).withColumn(transaction_date, to_date(transaction_date, yyyy-MM-dd))

total_spent_df = df.groupBy(user_id).agg(
    spark_sum(amount).alias(total_spent)
)

avg_transaction_df = df.groupBy(user_id).agg(
    avg(amount).alias(avg_transaction)
)

window_spec = Window.partitionBy(user_id).orderBy(desc(category_count))
favorite_category_df = df.groupBy(user_id, category).agg(
    count(category).alias(category_count)
).withColumn(
    rank, row_number().over(window_spec)
).filter(rank = 1).select(user_id, category)

total_spent_df.show()
avg_transaction_df.show()
favorite_category_df.show()
