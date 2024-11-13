from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, avg, col
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("CustomerTransactionAnalysis").getOrCreate()

data = [
    (1, "2024-11-01", 100.0),
    (1, "2024-11-02", 150.0),
    (1, "2024-11-05", 200.0),
    (2, "2024-11-01", 120.0),
    (2, "2024-11-03", 130.0),
    (2, "2024-11-08", 170.0),
]
schema = ["customer_id", "transaction_date", "amount"]
transactions = spark.createDataFrame(data, schema=schema)

]transactions = transactions.withColumn("transaction_date", col("transaction_date").cast("date"))

cumulative_window = Window.partitionBy("customer_id").orderBy("transaction_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
rolling_window = Window.partitionBy("customer_id").orderBy("transaction_date").rowsBetween(-6, Window.currentRow)

result_df = transactions.withColumn("cumulative_amount", spark_sum("amount").over(cumulative_window)) \
                        .withColumn("rolling_avg_amount", avg("amount").over(rolling_window))
result_df.show()
