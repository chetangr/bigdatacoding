# Databricks notebook source
# IMPORTS
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC 1) Imagine a student's test scores dataset, and you want to understand the relative performance of each student compared to others. This is where the PySpark PERCENT_RANK function comes into play.
# MAGIC
# MAGIC 🔑 Step 1️⃣: Framing the Task
# MAGIC Your goal is to calculate the percentile rank of each student's test score using PySpark's PERCENT_RANK function. This insight can provide valuable information about student's performance in comparison to their peers.
# MAGIC

# COMMAND ----------


spark = SparkSession.builder.appName("PercentRankFunctionExample").getOrCreate()

# Sample student data
data = [("A", 85),("B", 70),("C", 92),("D", 60),("E", 78)]

columns = ["student_name", "test_score"]
df = spark.createDataFrame(data, columns)

# Define the window specification
window_spec = Window.orderBy(F.desc("test_score"))

# Apply the PySpark PERCENT_RANK function
df_with_percent_rank = df.withColumn("percent_rank", F.percent_rank().over(window_spec))
df_with_percent_rank.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC 2)TIGERANALYTICS INTERVIEW QUESTION
# MAGIC
# MAGIC Convert
# MAGIC Name, Sub, Marks
# MAGIC ('Rudra','math',79),
# MAGIC ('Rudra','eng',60),
# MAGIC ('Shivu','math', 68),
# MAGIC ('Shivu','eng', 59),
# MAGIC ('Anu','math', 65),
# MAGIC ('Anu','eng',80)
# MAGIC
# MAGIC to 
# MAGIC Name, math, eng
# MAGIC ('Rudra',79,60),
# MAGIC ('Shivu', 68, 59),
# MAGIC ('Anu',65, 80)
# MAGIC
# MAGIC data=[
# MAGIC ('Rudra','math',79),
# MAGIC ('Rudra','eng',60),
# MAGIC ('Shivu','math', 68),
# MAGIC ('Shivu','eng', 59),
# MAGIC ('Anu','math', 65),
# MAGIC ('Anu','eng',80)
# MAGIC ]
# MAGIC schema="Name string,Sub string,Marks int"
# MAGIC df=spark.createDataFrame(data,schema)
# MAGIC df.show()

# COMMAND ----------

data=[
('Rudra','math',79),
('Rudra','eng',60),
('Shivu','math', 68),
('Shivu','eng', 59),
('Anu','eng',80),
('Anu','math', 65)
]
schema="Name string,Sub string,Marks int"
df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df.groupBy(F.col("Name")).pivot("Sub",["math","eng"]).agg(F.first(df.Marks)).show()

# COMMAND ----------


