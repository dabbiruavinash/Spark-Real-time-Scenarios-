Here's the solution for both PySpark and Oracle SQL to calculate daily average order_value per customer and 7-day rolling average.

Assume table/DataFrame has columns: order_date, cust_name, order_value

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, round, lag, rowsBetween, current_row
from pyspark.sql.window import Window
from pyspark.sql.types import DateType, DecimalType

# Sample data preparation (assuming you have a DataFrame 'df')
# df columns: order_date, cust_name, order_value

# ========== 1. Daily Average Order Value per Customer ==========
daily_avg = df.groupBy("cust_name", "order_date") \
    .agg(
        round(avg("order_value"), 2).alias("daily_avg_order_value"),
        sum("order_value").alias("total_daily_value"),
        count("order_value").alias("num_orders")
    ) \
    .orderBy("cust_name", "order_date")

print("Daily Average Order Value per Customer:")
daily_avg.show()

# ========== 2. 7-Day Rolling Average per Customer ==========
# Define window specification for 7-day rolling (including current day)
window_spec = Window.partitionBy("cust_name") \
    .orderBy("order_date") \
    .rowsBetween(-6, 0)  # Previous 6 days + current day = 7 days total

# First calculate daily aggregates per customer, then rolling average
daily_aggregates = df.groupBy("cust_name", "order_date") \
    .agg(avg("order_value").alias("daily_avg"))

rolling_7day = daily_aggregates.withColumn(
    "rolling_7day_avg",
    round(avg("daily_avg").over(window_spec), 2)
)

print("\n7-Day Rolling Average per Customer:")
rolling_7day.orderBy("cust_name", "order_date").show()

# Alternative: If you need rolling average at transaction level
rolling_transaction_level = df.withColumn(
    "rolling_7day_avg_transaction",
    round(avg("order_value").over(window_spec), 2)
)

rolling_transaction_level.orderBy("cust_name", "order_date").show()

# ========== 3. Complete Output with Daily and Rolling ==========
final_result = daily_aggregates.withColumn(
    "rolling_7day_avg",
    round(avg("daily_avg").over(window_spec), 2)
)

final_result.orderBy("cust_name", "order_date").createOrReplaceTempView("customer_metrics")

# Display final results
final_result.orderBy("cust_name", "order_date").show(50, truncate=False)

%sql
df.createOrReplaceTempView("orders")

# Daily average
daily_avg_sql = spark.sql("""
    SELECT 
        cust_name,
        order_date,
        ROUND(AVG(order_value), 2) AS daily_avg_order_value,
        SUM(order_value) AS total_daily_value,
        COUNT(*) AS num_orders
    FROM orders
    GROUP BY cust_name, order_date
    ORDER BY cust_name, order_date
""")
daily_avg_sql.show()

# 7-day rolling average using SQL
rolling_sql = spark.sql("""
    WITH daily_agg AS (
        SELECT 
            cust_name,
            order_date,
            AVG(order_value) AS daily_avg
        FROM orders
        GROUP BY cust_name, order_date
    )
    SELECT 
        cust_name,
        order_date,
        daily_avg,
        ROUND(
            AVG(daily_avg) OVER (
                PARTITION BY cust_name 
                ORDER BY order_date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 2
        ) AS rolling_7day_avg
    FROM daily_agg
    ORDER BY cust_name, order_date
""")
rolling_sql.show()