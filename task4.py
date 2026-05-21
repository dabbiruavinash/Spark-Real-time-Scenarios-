you have a dataframe of user login data with user_id and login_date
login streak is defined as series of consecutive login days
your task is to calculate the current running streak for each user on each day they log in.
for example, if a user logs in on jan 1,2,3,4,5,6 their streaks would be 1,2,1,2,3 respectively

Solution 1: Using lag() and Window Functions (Recommended

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when, sum as _sum, row_number, datediff, to_date
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DateType

# Sample data preparation
data = [
    ("user1", "2024-01-01"),
    ("user1", "2024-01-02"),
    ("user1", "2024-01-03"),
    ("user1", "2024-01-04"),
    ("user1", "2024-01-05"),
    ("user1", "2024-01-06"),  # Consecutive 6 days
    ("user2", "2024-01-01"),
    ("user2", "2024-01-02"),
    ("user2", "2024-01-04"),
    ("user2", "2024-01-05"),
    ("user2", "2024-01-06"),  # Gap on Jan 3
    ("user3", "2024-01-01"),
    ("user3", "2024-01-03"),
    ("user3", "2024-01-05"),  # Alternating days
]

df = spark.createDataFrame(data, ["user_id", "login_date"])
df = df.withColumn("login_date", to_date("login_date"))

# ========== SOLUTION 1: Group Island Approach ==========
# Step 1: Create a window partitioned by user, ordered by date
window_spec = Window.partitionBy("user_id").orderBy("login_date")

# Step 2: Get previous login date
df_with_prev = df.withColumn("prev_date", lag("login_date").over(window_spec))

# Step 3: Identify gap (if difference > 1 day, it's a new streak)
df_with_gap = df_with_prev.withColumn(
    "is_new_streak",
    when(
        datediff(col("login_date"), col("prev_date")) != 1, 1
    ).otherwise(0)
)

# Step 4: Create streak group ID using cumulative sum
df_with_group = df_with_gap.withColumn(
    "streak_group",
    _sum("is_new_streak").over(window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow))
)

# Step 5: Calculate running streak within each group
df_with_streak = df_with_group.withColumn(
    "running_streak",
    row_number().over(Window.partitionBy("user_id", "streak_group").orderBy("login_date"))
)

# Show results
print("Solution 1 - Running Streak per User per Login Date:")
df_with_streak.select("user_id", "login_date", "running_streak").orderBy("user_id", "login_date").show()

# Output:
# +-------+----------+---------------+
# |user_id|login_date|running_streak |
# +-------+----------+---------------+
# | user1 |2024-01-01|      1        |
# | user1 |2024-01-02|      2        |
# | user1 |2024-01-03|      3        |
# | user1 |2024-01-04|      4        |
# | user1 |2024-01-05|      5        |
# | user1 |2024-01-06|      6        |
# | user2 |2024-01-01|      1        |
# | user2 |2024-01-02|      2        |
# | user2 |2024-01-04|      1        | <- Reset because Jan 3 missing
# | user2 |2024-01-05|      2        |
# | user2 |2024-01-06|      3        |
# | user3 |2024-01-01|      1        |
# | user3 |2024-01-03|      1        | <- Reset (gap)
# | user3 |2024-01-05|      1        | <- Reset (gap)
# +-------+----------+---------------+


Solution 2: Using Date Difference and Conditional Logic

# Alternative approach using conditional streak calculation

# Step 1: Create row number for each user login (ranked by date)
window_spec = Window.partitionBy("user_id").orderBy("login_date")
df_with_rn = df.withColumn("rn", row_number().over(window_spec))

# Step 2: Create date group by subtracting row number from date
# For consecutive dates, login_date - rn will be constant
df_with_group = df_with_rn.withColumn(
    "date_group",
    datediff(col("login_date"), col("rn"))
)

# Step 3: Calculate streak within each group
df_with_streak = df_with_group.withColumn(
    "running_streak",
    row_number().over(Window.partitionBy("user_id", "date_group").orderBy("login_date"))
)

print("Solution 2 - Using Date Difference Method:")
df_with_streak.select("user_id", "login_date", "running_streak").orderBy("user_id", "login_date").show()

Solution 3: Using SQL Syntax (Easier to Understand)

# Register temp view for SQL queries
df.createOrReplaceTempView("user_logins")

streak_sql = spark.sql("""
    WITH login_with_prev AS (
        SELECT 
            user_id,
            login_date,
            LAG(login_date) OVER (PARTITION BY user_id ORDER BY login_date) AS prev_date
        FROM user_logins
    ),
    streak_groups AS (
        SELECT 
            user_id,
            login_date,
            SUM(
                CASE 
                    WHEN DATEDIFF(login_date, prev_date) = 1 THEN 0 
                    ELSE 1 
                END
            ) OVER (PARTITION BY user_id ORDER BY login_date) AS streak_group
        FROM login_with_prev
    )
    SELECT 
        user_id,
        login_date,
        ROW_NUMBER() OVER (PARTITION BY user_id, streak_group ORDER BY login_date) AS running_streak
    FROM streak_groups
    ORDER BY user_id, login_date
""")

print("Solution 3 - Using SQL Syntax:")
streak_sql.show()

Solution 4: Using reduce (MapReduce style for large datasets)

from pyspark.sql.functions import collect_list, struct, expr
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType

# Alternative: Using higher-order functions (Spark 3.0+)
def calculate_streak_with_transform(df):
    """
    Using transform function to calculate streak within array
    """
    
    # Collect logins per user as array
    df_grouped = df.groupBy("user_id").agg(
        collect_list(struct("login_date")).alias("logins")
    )
    
    # UDF to calculate running streaks
    from pyspark.sql.functions import udf
    from datetime import date, timedelta
    
    def compute_running_streak(logins_list):
        if not logins_list:
            return []
        
        # Sort by date
        sorted_logins = sorted(logins_list, key=lambda x: x["login_date"])
        result = []
        current_streak = 0
        prev_date = None
        
        for login in sorted_logins:
            login_date = login["login_date"]
            if prev_date is None or (login_date - prev_date).days == 1:
                current_streak += 1
            else:
                current_streak = 1
            result.append({"login_date": login_date, "streak": current_streak})
            prev_date = login_date
        
        return result
    
    compute_streak_udf = udf(compute_running_streak, 
                            ArrayType(StructType([
                                StructField("login_date", DateType(), True),
                                StructField("streak", IntegerType(), True)
                            ])))
    
    df_result = df_grouped.withColumn("streaks", compute_streak_udf(col("logins")))
    
    # Explode back to original format
    from pyspark.sql.functions import explode
    df_final = df_result.select("user_id", explode("streaks").alias("streak_data"))
    df_final = df_final.select("user_id", "streak_data.login_date", "streak_data.streak")
    
    return df_final

# Usage
df_final = calculate_streak_with_transform(df)
print("Solution 4 - Using UDF with Array Transform:")
df_final.orderBy("user_id", "login_date").show()


