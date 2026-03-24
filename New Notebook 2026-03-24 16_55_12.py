# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.firstprojectdads.blob.core.windows.net",
  "aOdSpciyQXImgNBE1mil485o7Q7oUxvhkQ+flGONasTOUw7xVeJAPr0IyymPrm6eDntxvFq9qy0o+ASt6byqjg=="
)

# COMMAND ----------

display(dbutils.fs.ls("wasbs://retail@firstprojectdads.blob.core.windows.net/"))

# COMMAND ----------

display(dbutils.fs.ls("wasbs://retail@firstprojectdads.blob.core.windows.net/bronze/"))

# COMMAND ----------

display(dbutils.fs.ls("wasbs://retail@firstprojectdads.blob.core.windows.net/bronze/product"))

# COMMAND ----------

# DBTITLE 1,read parquet file for specific data
df = spark.read.parquet(
    "wasbs://retail@firstprojectdads.blob.core.windows.net/bronze/store"
)

display(df)

# COMMAND ----------

# DBTITLE 1,read parquet file for specific data
df = spark.read.parquet(
    "wasbs://retail@firstprojectdads.blob.core.windows.net/bronze/product"
)

display(df)

# COMMAND ----------

# DBTITLE 1,display multiples files
df_product = spark.read.parquet(
    "wasbs://retail@firstprojectdads.blob.core.windows.net/bronze/product/"
)

df_store = spark.read.parquet(
    "wasbs://retail@firstprojectdads.blob.core.windows.net/bronze/store/"
)

df_transaction = spark.read.parquet(
    "wasbs://retail@firstprojectdads.blob.core.windows.net/bronze/transaction/"
)
df_customer = spark.read.parquet(
    "wasbs://retail@firstprojectdads.blob.core.windows.net/bronze/customer/yash-azure-aws/datpipeline/refs/heads/main/"

)

display(df_product)
display(df_store)
display(df_transaction)
display(df_customer)


# COMMAND ----------

# DBTITLE 1,display
display(df_transaction)

# COMMAND ----------

# DBTITLE 1,clean the data
from pyspark.sql.functions import col
# Convert types and clean data

df_transaction = df_transaction.select(
    col("transaction_id").cast("int"),
    col("customer_id").cast("int"),
    col("product_id").cast("int"),
    col("store_id").cast("int"),
    col("quantity").cast("int"),
    col("transaction_date").cast("date")
)

df_product = df_product.select(
    col("product_id").cast("int"),
    col("product_name"),
    col("category"),
    col("price").cast("double")
)

df_store = df_store.select(
    col("store_id").cast("int"),
    col("store_name"),
    col("location")
)

df_customer = df_customer.select(
    "customer_id", "first_name", "last_name", "email", "city", "registration_date"
).dropDuplicates(["customer_id"])


# COMMAND ----------

# DBTITLE 1,Join all data silver is created
# Join all data
df_silver = df_transaction \
    .join(df_customer, "customer_id") \
    .join(df_product, "product_id") \
    .join(df_store, "store_id") \
    .withColumn("total_amount", col("quantity") * col("price"))

# COMMAND ----------

# DBTITLE 1,display silver data set
# DBTITLE 1,display silver data set
display(df_silver)

# COMMAND ----------

# DBTITLE 1,dump all in the silver folder of lake
# DBTITLE 1,dump all in the silver folder of lake
# Set Azure storage key
spark.conf.set(
    "fs.azure.account.key.firstprojectdads.dfs.core.windows.net",
    "aOdSpciyQXImgNBE1mil485o7Q7oUxvhkQ+flGONasTOUw7xVeJAPr0IyymPrm6eDntxvFq9qy0o+ASt6byqjg=="
)

# Define path
silver_path = "abfss://retail@firstprojectdads.dfs.core.windows.net/silver/"

# Write data
df_silver.write \
    .mode("overwrite") \
    .format("delta") \
    .save(silver_path)

# COMMAND ----------

# DBTITLE 1,create dat set table
spark.conf.set(
    "fs.azure.account.key.firstprojectdads.dfs.core.windows.net",
    "aOdSpciyQXImgNBE1mil485o7Q7oUxvhkQ+flGONasTOUw7xVeJAPr0IyymPrm6eDntxvFq9qy0o+ASt6byqjg=="
)


# COMMAND ----------

dbutils.fs.ls("abfss://retail@firstprojectdads.dfs.core.windows.net/")

# COMMAND ----------

df = spark.read.format("delta").load(
    "abfss://retail@firstprojectdads.dfs.core.windows.net/silver/"
)

df.write.format("delta").mode("overwrite").saveAsTable("retail_silver_cleaned", path="abfss://retail@firstprojectdads.dfs.core.windows.net/silver/retail_silver_cleaned")


# COMMAND ----------

spark.sql(f"""
CREATE TABLE retail_silver_cleaned
USING DELTA
LOCATION 'abfss://retail@firstprojectdads.dfs.core.windows.net/silver/'
""")

# COMMAND ----------

# DBTITLE 1,DBTITLE 1,gold layer # Load cleaned transactions from Silver layer
DBTITLE 1,gold layer
# Load cleaned transactions from Silver layer


silver_df = spark.read.format("delta").load(
    "abfss://retail@firstprojectdads.dfs.core.windows.net/silver/"
)

# COMMAND ----------

display(silver_df)


# COMMAND ----------

from pyspark.sql.functions import sum, countDistinct, avg

gold_df = silver_df.groupBy(
    "transaction_date",
    "product_id", "product_name", "category",
    "store_id", "store_name", "location"
).agg(
    sum("quantity").alias("total_quantity_sold"),
    sum("total_amount").alias("total_sales_amount"),
    countDistinct("transaction_id").alias("number_of_transactions"),
    avg("total_amount").alias("average_transaction_value")
)


# COMMAND ----------

display(gold_df)

# COMMAND ----------

# DBTITLE 1,dump all in the silver folder of lake
# Set Azure storage key
spark.conf.set(
    "fs.azure.account.key.firstprojectdads.dfs.core.windows.net",
    "aOdSpciyQXImgNBE1mil485o7Q7oUxvhkQ+flGONasTOUw7xVeJAPr0IyymPrm6eDntxvFq9qy0o+ASt6byqjg=="
)

# Define path
gold_path = "abfss://retail@firstprojectdads.dfs.core.windows.net/gold/"

# Write data
gold_df.write \
    .mode("overwrite") \
    .format("delta") \
    .save(gold_path)