# Databricks notebook source
# MAGIC  %run /Workspace/Users/sejalkumbhar30@outlook.com/Retail_Inventory_system/Logger_files/decorators

# COMMAND ----------

# Databricks notebook source
from pyspark.sql.functions import col, when, current_timestamp, coalesce
from delta.tables import DeltaTable

class SilverTransformer:

    def __init__(self, logger, spark):
        self.logger = logger
        self.spark = spark

    @log_method
    def filter_new_files(self, bronze_df, silver_path):
        """
        Filter out already ingested files based on source_file.
        """
        if DeltaTable.isDeltaTable(self.spark, silver_path):
            silver_columns = self.spark.read.format("delta").load(silver_path).columns

            if "source_file" not in silver_columns:
                self.logger.info("source_file not found - MERGE will handle duplicates")
                return bronze_df

            already_loaded = self.spark.read.format("delta") \
                .load(silver_path) \
                .select("source_file") \
                .distinct()

            new_data = bronze_df.join(already_loaded, on="source_file", how="left_anti")
            return new_data
        else:
            self.logger.info("First load - no filtering needed")
            return bronze_df

    @log_method
    def clean(self, df):
        """
        Basic cleaning: drop nulls, cast columns, fill defaults.
        """
        df = df.dropna(subset=["product_id"])
        df = df.withColumn("brand", col("brand").cast("string")) \
               .withColumn("category", col("category").cast("string")) \
               .withColumn("title", col("title").cast("string"))
        df = df.fillna({
            "brand"    : "Unknown",
            "category" : "Uncategorized",
            "title"    : "Unknown",
            "stock"    : 0,
            "price"    : 0.0
        })
        df = df.withColumn("stock", col("stock").cast("long")) \
               .withColumn("price", col("price").cast("double"))
        df = df.dropDuplicates(["product_id", "source_file"])
        return df

    @log_method
    def split(self, df):
        """
        Split into product and inventory tables.
        """
        product_df = df.select(
            col("product_id"),
            col("title").alias("product_name"),
            col("category"),
            col("brand"),
            col("source_file")
        ).dropDuplicates(["product_id"])

        inventory_df = df.select(
            col("product_id"),
            col("stock"),
            col("price"),
            col("ingestion_time"),
            col("source_file")
        ).withColumn(
            "stock_status",
            when(col("stock") < 10, "Low")
            .when(col("stock") > 100, "Over")
            .otherwise("Normal")
        )

        return product_df, inventory_df

    @log_method
    def merge_incremental(self, df, target_path, timestamp_col="ingestion_time"):
        """
        Merge incrementally using timestamp-based logic.
        Ensures both new rows and updates are captured.
        """
        if not DeltaTable.isDeltaTable(self.spark, target_path):
            self.logger.info("First load - writing directly")
            df.write.format("delta").mode("overwrite").save(target_path)
        else:
            delta_table = DeltaTable.forPath(self.spark, target_path)

            # Upsert based on product_id and timestamp
            merge_condition = f"target.product_id = source.product_id AND source.{timestamp_col} > target.{timestamp_col}"

            delta_table.alias("target").merge(
                df.alias("source"),
                merge_condition
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()

    @log_method
    def detect_stock(self, inventory_df):
        """
        Detect low stock and overstock alerts with business thresholds.
        """
        # Low stock alerts
        low_stock = inventory_df.filter(col("stock") < 10) \
            .withColumn(
                "alert_level",
                when(col("stock") == 0, "Critical")
                .when(col("stock") <= 3, "High")
                .otherwise("Medium")
            ) \
            .withColumn("alert_time", current_timestamp()) \
            .withColumn(
                "reorder_qty",
                when(col("stock") == 0, 100)
                .when(col("stock") <= 3, 75)
                .otherwise(50)
            )

        # Overstock alerts
        over_stock = inventory_df.filter(col("stock") > 100) \
            .withColumn(
                "alert_level",
                when(col("stock") > 200, "Critical")
                .when(col("stock") > 150, "High")
                .otherwise("Medium")
            ) \
            .withColumn("alert_time", current_timestamp()) \
            .withColumn(
                "reorder_qty",
                when(col("stock") > 200, 0)  # maybe hold orders
                .when(col("stock") > 150, 0)
                .otherwise(0)
            )

        return low_stock, over_stock

# COMMAND ----------



# COMMAND ----------

