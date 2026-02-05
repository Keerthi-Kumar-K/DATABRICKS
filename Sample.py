# Databricks notebook source

df = spark.read.table("workspace.default.mock_patient")
display(df)


# COMMAND ----------

df_clean=df

# COMMAND ----------

display(df_clean)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, bround, format_number
df_clean = df_clean.withColumn("Age_Upd",round(col("Age"),2))

# COMMAND ----------

df_clean.show(5)

# COMMAND ----------

df_order = spark.read.table("workspace.default.mock_patient_orders_data")
display(df_order)
df = spark.read.table("workspace.default.mock_patient")
display(df)

# COMMAND ----------

joined_df = df_clean.join(df_order, df_clean.patient_id == df_order.patient_id, "inner")
display(joined_df)

# COMMAND ----------

output_df=joined_df.groupBy("department","order_Month").count()
display(output_df)

# COMMAND ----------

# DBTITLE 1,Cell 9
from pyspark.sql.functions import date_format,to_date,try_to_date,coalesce,lit
#Generate a SQL query to display all patients over the age of 40 who had an order for a procedure in the Radiology department and were discharged with the reason ‘Transferred’. Include the following details: full name, age, birth date (MM/DD), procedure, discharge date (Month D, YYYY), visit type, and inpatient as “YES/NO”. Order the results by procedure name and by descending patient age.

df_filtered_join = joined_df.filter((joined_df.Age_Upd > 40) & (joined_df.department == "Radiology") & (joined_df.reason=="Transferred"))
df_final = df_filtered_join.select("full_name","Age_Upd",date_format("birth_date", "MM/dd").alias("birth_date" ),"procedure",coalesce(date_format(try_to_date("discharged_date", "MM/dd/yy"), "MMMM d, yyyy"),lit("Invalid_Format")).alias("discharged_date"),when(col("InpatientFlag")==1,"YES").otherwise("NO").alias("inpatient"))
display(df_final)

# COMMAND ----------

import pyspark
print(pyspark.__version__)

# COMMAND ----------

