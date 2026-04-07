# Databricks notebook source
# MAGIC %run /Workspace/Users/sejalkumbhar30@outlook.com/Retail_Inventory_system/Logger_files/logger.py
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

from pyspark.sql.functions import col, sum, avg, count

class GoldTransformer:

    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger

    @log_method
    def load_silver(self):
        product_df    = self.spark.table("retail_catalog.silver.products")
        inventory_df  = self.spark.table("retail_catalog.silver.inventory")
        low_stock_df  = self.spark.table("retail_catalog.silver.low_stock")
        over_stock_df = self.spark.table("retail_catalog.silver.over_stock")
        return product_df, inventory_df, low_stock_df, over_stock_df

    @log_method
    def inventory_summary(self, inventory_df):
        return inventory_df.groupBy("product_id").agg(
            sum("stock").alias("total_stock"),
            avg("price").alias("avg_price")
        )

    @log_method
    def category_analysis(self, product_df, inventory_df):
        return product_df.join(inventory_df, "product_id") \
            .groupBy("category") \
            .agg(sum("stock").alias("total_stock"))

    @log_method
    def stock_alerts(self, low_stock_df, over_stock_df):
        low_count  = low_stock_df.count()
        over_count = over_stock_df.count()
        data = [(low_count, over_count)]
        return self.spark.createDataFrame(data, ["low_stock_count", "over_stock_count"])

    @log_method
    def low_stock_detail(self, product_df, low_stock_df):
        self.logger.info("Creating low stock detail report")
        return product_df.join(low_stock_df, "product_id") \
            .select(
                col("product_id"),
                col("product_name"),
                col("category"),
                col("brand"),
                col("stock"),
                col("price"),
                col("stock_status"),
                col("alert_level"),   
                col("reorder_qty")   
            ) \
            .fillna("Unknown", subset=["brand", "category"]) \
            .orderBy("stock")


    @log_method
    def category_stock_summary(self, product_df, inventory_df):
        return product_df.join(inventory_df, "product_id") \
            .groupBy("category") \
            .agg(
                count("product_id").alias("total_products"),
                sum("stock").alias("total_stock"),
                avg("price").alias("avg_price")
            ).orderBy("total_stock")

# COMMAND ----------

logger, log_file = get_logger("GoldPipeline","gold")

transformer = GoldTransformer(spark, logger)
writer = SilverWriter(logger, spark)  


BASE_PATH = "abfss://input@retailstoragesejal.dfs.core.windows.net/Gold/"

SUMMARY_PATH = BASE_PATH + "inventory_summary/"
CATEGORY_PATH = BASE_PATH + "category_analysis/"
ALERT_PATH = BASE_PATH + "stock_alerts/"
LOW_STOCK_DETAIL_PATH  = BASE_PATH + "low_stock_detail/"
CATEGORY_SUMMARY_PATH  = BASE_PATH + "category_stock_summary/"

try:
    logger.info("===== Gold Pipeline Started =====")


    product_df, inventory_df, low_stock_df, over_stock_df = transformer.load_silver()

    # Step 2: Transform
    summary_df = transformer.inventory_summary(inventory_df)
    category_df = transformer.category_analysis(product_df, inventory_df)
    alert_df = transformer.stock_alerts(low_stock_df, over_stock_df)
    low_stock_detail_df   = transformer.low_stock_detail(product_df, low_stock_df)
    category_summary_df   = transformer.category_stock_summary(product_df, inventory_df)
    alerts_df = transformer.stock_alerts(low_stock_df, over_stock_df)

    # Step 3: Write Gold
    writer.overwrite(summary_df, SUMMARY_PATH)
    writer.overwrite(category_df, CATEGORY_PATH)
    writer.overwrite(alert_df, ALERT_PATH)
    writer.overwrite(low_stock_detail_df,  LOW_STOCK_DETAIL_PATH)
    writer.overwrite(category_summary_df,  CATEGORY_SUMMARY_PATH)


    # # Step 4: Create Tables
    writer.create_table("retail_catalog.gold.inventory_summary", SUMMARY_PATH)
    writer.create_table("retail_catalog.gold.category_analysis", CATEGORY_PATH)
    writer.create_table("retail_catalog.gold.stock_alerts", ALERT_PATH)
    writer.create_table("retail_catalog.gold.low_stock_detail",     LOW_STOCK_DETAIL_PATH)
    writer.create_table("retail_catalog.gold.category_stock_summary", CATEGORY_SUMMARY_PATH)


    logger.info("===== Gold Pipeline Completed =====")
    upload_logs_to_adls(log_file, "gold")


except Exception as e:
    logger.error(f"Gold Pipeline failed: {str(e)}")
    upload_logs_to_adls(log_file, "gold")

    raise

# COMMAND ----------

low_count = alerts_df.collect()[0]["low_stock_count"]

dbutils.notebook.exit(str(low_count))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM retail_catalog.gold.inventory_summary;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`abfss://input@retailstoragesejal.dfs.core.windows.net/Gold/category_analysis/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`abfss://input@retailstoragesejal.dfs.core.windows.net/Gold/stock_alerts/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`abfss://input@retailstoragesejal.dfs.core.windows.net/Gold/low_stock_detail/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM retail_catalog.gold.low_stock_detail;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`abfss://input@retailstoragesejal.dfs.core.windows.net/Gold/category_stock_summary/`
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

