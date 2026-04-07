# Databricks notebook source
# MAGIC %run /Workspace/Users/sejalkumbhar30@outlook.com/Retail_Inventory_system/Logger_files/logger.py
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run /Workspace/Users/sejalkumbhar30@outlook.com/Retail_Inventory_system/Logger_files/log_utils.ipynb

# COMMAND ----------

# MAGIC  %run /Workspace/Users/sejalkumbhar30@outlook.com/Retail_Inventory_system/Silver/Silver_reader.ipynb

# COMMAND ----------

# MAGIC %run /Workspace/Users/sejalkumbhar30@outlook.com/Retail_Inventory_system/Silver/Silver_transformer.ipynb

# COMMAND ----------

# MAGIC %run /Workspace/Users/sejalkumbhar30@outlook.com/Retail_Inventory_system/Silver/Silver_writer.ipynb

# COMMAND ----------

logger, log_file = get_logger("SilverPipeline", "silver")
reader = SilverReader(spark, "retail_catalog.bronze.retail_table", logger)
transformer = SilverTransformer(logger, spark)
writer = SilverWriter(logger, spark)

PRODUCT_PATH   = "abfss://input@retailstoragesejal.dfs.core.windows.net/Silver/products/"
INVENTORY_PATH = "abfss://input@retailstoragesejal.dfs.core.windows.net/Silver/inventory/"
LOW_STOCK_PATH = "abfss://input@retailstoragesejal.dfs.core.windows.net/Silver/low_stock/"
OVER_STOCK_PATH = "abfss://input@retailstoragesejal.dfs.core.windows.net/Silver/over_stock/"

try:
    logger.info("===== Silver Pipeline Started =====")

    df = reader.read()

    full_clean_df = transformer.clean(df)
    full_product_df, _ = transformer.split(full_clean_df)
    writer.overwrite(full_product_df, PRODUCT_PATH)
    logger.info(f"Products written: {full_product_df.count()}")

    new_df = transformer.filter_new_files(df, INVENTORY_PATH)
    new_df = transformer.clean(new_df)
    _, inventory_df = transformer.split(new_df)
    inventory_df = inventory_df.withColumn("stock", col("stock").cast("long"))
    transformer.merge_incremental(inventory_df, INVENTORY_PATH)

    full_inventory_df = spark.read.format("delta").load(INVENTORY_PATH)

    low_stock, over_stock = transformer.detect_stock(full_inventory_df)
    writer.overwrite(low_stock, LOW_STOCK_PATH)
    writer.overwrite(over_stock, OVER_STOCK_PATH)

    writer.create_table("retail_catalog.silver.products",   PRODUCT_PATH)
    writer.create_table("retail_catalog.silver.inventory",  INVENTORY_PATH)
    writer.create_table("retail_catalog.silver.low_stock",  LOW_STOCK_PATH)
    writer.create_table("retail_catalog.silver.over_stock", OVER_STOCK_PATH)

    upload_logs_to_adls(log_file, "silver")
    logger.info("===== Silver Pipeline Completed =====")

except Exception as e:
    logger.error(f"Pipeline failed: {str(e)}")
    raise

# COMMAND ----------

spark.read.table("retail_catalog.silver.low_stock").display()



# COMMAND ----------

spark.read.table("retail_catalog.silver.over_stock").display()

# COMMAND ----------

low  = spark.read.table("retail_catalog.silver.low_stock")
prod = spark.read.table("retail_catalog.silver.products")

low.join(prod, on="product_id") \
   .select("product_name", "category", "brand", "stock", "alert_level", "reorder_qty") \
   .display()

# COMMAND ----------

spark.read.table("retail_catalog.silver.inventory").count()

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col

delta_table = DeltaTable.forPath(
    spark,
    "abfss://input@retailstoragesejal.dfs.core.windows.net/Silver/inventory/"
)

history_df = delta_table.history()

history_df.select(
    "timestamp",
    col("operationMetrics.numTargetRowsInserted").alias("inserted"),
    col("operationMetrics.numTargetRowsUpdated").alias("updated")
).filter("operation = 'MERGE'").show()

# COMMAND ----------

