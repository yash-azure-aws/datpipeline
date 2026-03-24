# Databricks notebook source
# Databricks notebook source
try:
    dbutils.fs.mount(
        source="wasbs://retail@demopipelinebricks.blob.core.windows.net",
        mount_point="/mnt/retail",
        extra_configs={
            "fs.azure.account.key.demopipelinebricks.blob.core.windows.net":
            "BLSgfLeR1Cp7kD+aVuPCWZFHf9IYAN7l2VQcX7vcli6U5xoY7snutUWck2+4vOnI2nYcp/tZZsx6+AStYJrv1A=="
        }
    )
    print("Mount successful")
except Exception as e:
    print(f"Mount failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC credential

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.demopipelinebricks.blob.core.windows.net",
  "BLSgfLeR1Cp7kD+aVuPCWZFHf9IYAN7l2VQcX7vcli6U5xoY7snutUWck2+4vOnI2nYcp/tZZsx6+AStYJrv1A=="
)

# COMMAND ----------

# MAGIC %md
# MAGIC display folders

# COMMAND ----------

display(dbutils.fs.ls("wasbs://retail@demopipelinebricks.blob.core.windows.net/"))

# COMMAND ----------

# MAGIC %md
# MAGIC display specific folder

# COMMAND ----------

display(dbutils.fs.ls("wasbs://retail@demopipelinebricks.blob.core.windows.net/bronze/"))

# COMMAND ----------


read parquet file for specific data

# COMMAND ----------

# MAGIC %md
# MAGIC read spefic file

# COMMAND ----------

# DBTITLE 1,read bronze
df = spark.read.parquet(
    "wasbs://retail@demopipelinebricks.blob.core.windows.net/bronze/product/",
   
    
    
)
display(df)

# COMMAND ----------

# DBTITLE 1,multiples read
df_product = spark.read.parquet(
    "wasbs://retail@demopipelinebricks.blob.core.windows.net/bronze/product/"
)

df_store = spark.read.parquet(
    "wasbs://retail@demopipelinebricks.blob.core.windows.net/bronze/store/"
)

df_transaction = spark.read.parquet(
    "wasbs://retail@demopipelinebricks.blob.core.windows.net/bronze/transaction/"
)
df_customer = spark.read.parquet(
    "wasbs://retail@demopipelinebricks.blob.core.windows.net/bronze/customer/yash-azure-aws/datpipeline/refs/heads/main/"
)

display(df_product)
display(df_store)
display(df_transaction)
display(df_customer)

# COMMAND ----------

# DBTITLE 1,multiple files read
spark.conf.set(
  "fs.azure.account.key.demopipelinebricks.blob.core.windows.net",
  "BLSgfLeR1Cp7kD+aVuPCWZFHf9IYAN7l2VQcX7vcli6U5xoY7snutUWck2+4vOnI2nYcp/tZZsx6+AStYJrv1A=="
)

df = spark.read.parquet(
    "wasbs://retail@demopipelinebricks.blob.core.windows.net/bronze/product/",
    "wasbs://retail@demopipelinebricks.blob.core.windows.net/bronze/store/",
    "wasbs://retail@demopipelinebricks.blob.core.windows.net/bronze/transaction/"
)

display(df)

# COMMAND ----------

display(df_transaction)

# COMMAND ----------

# DBTITLE 1,clean the data
from pyspark.sql.functions import col# Convert types and clean data

df_transactions = df_transaction.select(
    col("transaction_id").cast("int"),
    col("customer_id").cast("int"),
    col("product_id").cast("int"),
    col("store_id").cast("int"),
    col("quantity").cast("int"),
    col("transaction_date").cast("date")
)

df_products = df_product.select(
    col("product_id").cast("int"),
    col("product_name"),
    col("category"),
    col("price").cast("double")
)

df_stores = df_store.select(
    col("store_id").cast("int"),
    col("store_name"),
    col("location")
)

df_customers = df_customer.select(
    "customer_id", "first_name", "last_name", "email", "city", "registration_date"
).dropDuplicates(["customer_id"])

# COMMAND ----------

# DBTITLE 1,join all data together
# Join all data
df_silver = df_transactions \
    .join(df_customer, "customer_id") \
    .join(df_product, "product_id") \
    .join(df_store, "store_id") \
    .withColumn("total_amount", col("quantity") * col("price"))


# COMMAND ----------

# DBTITLE 1,display silver data set
display(df_silver)

# COMMAND ----------

# DBTITLE 1,,dump to adls location key with storage location
spark.conf.set(
  "fs.azure.account.key.demopipelinebricks.blob.core.windows.net",
  "BLSgfLeR1Cp7kD+aVuPCWZFHf9IYAN7l2VQcX7vcli6U5xoY7snutUWck2+4vOnI2nYcp/tZZsx6+AStYJrv1A=="
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://retail@demopipelinebricks.dfs.core.windows.net/"))

# COMMAND ----------

# DBTITLE 1,dump all in the silver folder of lake
silver_path = "abfss://retail@demopipelinebricks.dfs.core.windows.net/silver/"

df_silver.write \
    .mode("overwrite") \
    .format("delta") \
    .save(silver_path)

# COMMAND ----------

# DBTITLE 1,create dat set table
df = spark.read.format("delta").load(
    "abfss://retail@demopipelinebricks.dfs.core.windows.net/silver/"
)

df.write.format("delta").mode("overwrite").saveAsTable("retail_silver_cleaned")

# COMMAND ----------

# DBTITLE 1,display sql silver
# MAGIC %sql
# MAGIC SELECT * FROM retail_silver_cleaned;

# COMMAND ----------

# DBTITLE 1,gold layer  read the silver dataset
# Load cleaned transactions from Silver layer
silver_df = spark.read.format("delta").load(
    "abfss://retail@demopipelinebricks.dfs.core.windows.net/silver/"
)
 

# COMMAND ----------

display(silver_df)

# COMMAND ----------

# DBTITLE 1,creating gold from the silver data but limited things
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

# DBTITLE 1,display gold
display(gold_df)

# COMMAND ----------

# Load cleaned transactions from Silver layer
gold_df_df = spark.read.format("delta").load(
    "abfss://retail@demopipelinebricks.dfs.core.windows.net/gold/"
)

# COMMAND ----------

# DBTITLE 1,dump gold in azure lake
gold_path = "abfss://retail@demopipelinebricks.dfs.core.windows.net/gold/"

df_gold.write \
    .mode("overwrite") \
    .format("delta") \
    .save(gold_path)

# COMMAND ----------

# DBTITLE 1,create a table top of this
df = spark.read.format("delta").load(
    "abfss://retail@demopipelinebricks.dfs.core.windows.net/gold/"
)

df.write.format("delta").mode("overwrite").saveAsTable("retail_gold_final")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM retail_gold_final
# MAGIC