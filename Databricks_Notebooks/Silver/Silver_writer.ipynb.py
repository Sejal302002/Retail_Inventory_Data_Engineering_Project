# Databricks notebook source
# MAGIC  %run /Workspace/Users/sejalkumbhar30@outlook.com/Retail_Inventory_system/Logger_files/decorators

# COMMAND ----------

# Databricks notebook source
class SilverWriter:

    def __init__(self, logger, spark):
        self.logger = logger
        self.spark = spark

    @log_method
    def write(self, df, path):
        df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(path)

    @log_method
    def overwrite(self, df, path):
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(path)

    @log_method
    def create_table(self, table_name, path):
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            USING DELTA
            LOCATION '{path}'
        """)

# COMMAND ----------


