# Databricks notebook source
# MAGIC  %run /Workspace/Users/sejalkumbhar30@outlook.com/Retail_Inventory_system/Logger_files/decorators

# COMMAND ----------

# Databricks notebook source
class SilverReader:

    def __init__(self, spark, table_name, logger):
        self.spark = spark
        self.table_name = table_name
        self.logger = logger

    @log_method
    def read(self):
        df = self.spark.read.table(self.table_name)
        return df

# COMMAND ----------

