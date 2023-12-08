# Databricks notebook source
# MAGIC %md
# MAGIC # How to load data from Azure Blob Storage 
# MAGIC This notebook shows you how to create and query a table or DataFrame loaded from data stored in Azure Blob storage.
# MAGIC
# MAGIC ### Sources
# MAGIC https://docs.databricks.com/en/_extras/notebooks/source/data-import/azure-blob-store.html<br>
# MAGIC https://docs.databricks.com/en/storage/azure-storage.html
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 1: Set the data location and type
# MAGIC
# MAGIC There are 3 ways to access Azure Blob storage:
# MAGIC - OAuth 2.0 with an Azure service principal
# MAGIC - Shared access signatures (SAS)
# MAGIC - Account keys (this exemple)
# MAGIC
# MAGIC See https://docs.databricks.com/en/storage/azure-storage.html
# MAGIC
# MAGIC ![Associate-badge](https://www.databricks.com/wp-content/uploads/2022/04/associate-badge-eng.svg)
# MAGIC ![test](https://github.com/BaseConsulting/databricks-camp/blob/main/How%20to/img/azure_storage_access_keys.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ![Associate-badge](https://www.databricks.com/wp-content/uploads/2022/04/associate-badge-eng.svg)
# MAGIC ![test](https://github.com/BaseConsulting/databricks-camp/blob/main/How%20to/img/azure_storage_access_keys.jpg)

# COMMAND ----------

# MAGIC %fs ls '/databricks-datasets'

# COMMAND ----------

storage_account_name = "stacdatabrickscamp01"
storage_account_access_key = "bSZgQaFedsfshh59qwG+Elb9dq2+qso0EMwCtGcXaql+ZVt7ZIuH+KDQsqFlfh1YwXHgvYWptf/N+AStHjCpcQ=="

# COMMAND ----------

file_location = "wasbs://databricks-in/Titanic"
file_type = "csv"
container_name = "databricks-in"
csv_filename = "Titanic.csv"

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 2: Read the data
# MAGIC
# MAGIC Now that we have specified our file metadata, we can create a DataFrame. Notice that we use an *option* to specify that we want to infer the schema from the file. We can also explicitly set this to a particular schema if we have one already.
# MAGIC
# MAGIC First, let's create a DataFrame in Python.

# COMMAND ----------

df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)

# COMMAND ----------



# COMMAND ----------

# Load the CSV file into a Spark DataFrame
df = spark.read.format("csv") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .load(f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/{csv_filename}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 3: Query the data
# MAGIC
# MAGIC Now that we have created our DataFrame, we can query it. For instance, you can identify particular columns to select and display.

# COMMAND ----------

df.show()

# COMMAND ----------

display(df.select("PassengerId"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 4: (Optional) Create a view or table
# MAGIC
# MAGIC If you want to query this data as a table, you can simply register it as a *view* or a table.

# COMMAND ----------

df.createOrReplaceTempView("YOUR_TEMP_VIEW_NAME")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We can query this view using Spark SQL. For instance, we can perform a simple aggregation. Notice how we can use `%sql` to query the view from SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT EXAMPLE_GROUP, SUM(EXAMPLE_AGG) FROM YOUR_TEMP_VIEW_NAME GROUP BY EXAMPLE_GROUP

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Since this table is registered as a temp view, it will be available only to this notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.

# COMMAND ----------

df.write.format("parquet").saveAsTable("MY_PERMANENT_TABLE_NAME")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This table will persist across cluster restarts and allow various users across different notebooks to query this data.
