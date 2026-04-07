# Databricks notebook source
# MAGIC %run /Workspace/Users/sejalkumbhar30@outlook.com/Retail_Inventory_system/Logger_files/logger.py

# COMMAND ----------

# MAGIC %run /Workspace/Users/sejalkumbhar30@outlook.com/Retail_Inventory_system/Logger_files/log_utils.ipynb

# COMMAND ----------

# MAGIC  %run /Workspace/Users/sejalkumbhar30@outlook.com/Retail_Inventory_system/Logger_files/decorators

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, explode, input_file_name, col
from delta.tables import DeltaTable

class BronzeLayer:

    def __init__(self, spark, input_path, output_path, table_name):
        self.spark = spark
        self.input_path = input_path
        self.output_path = output_path
        self.table_name = table_name
        self.logger, self.log_file = get_logger(self.__class__.__name__, "bronze")

    @log_method
    def read_json(self):
        df = self.spark.read.option("multiline", "true").json(self.input_path)
        return df

    @log_method
    def flatten_json(self, df):
        df = df.select(explode("products").alias("products")).select("products.*")
        return df

    @log_method
    def add_metadata(self, df):
        df = df.withColumn("ingestion_time", current_timestamp())
        df = df.withColumn("source_file", input_file_name())
        df = df.withColumnRenamed("id", "product_id")

        # Cast to consistent types to avoid schema mismatch between files
        df = df.withColumn("price", col("price").cast("double"))
        df = df.withColumn("stock", col("stock").cast("long"))
        df = df.withColumn("discountPercentage", col("discountPercentage").cast("double"))
        df = df.withColumn("rating", col("rating").cast("double"))
        df = df.withColumn("weight", col("weight").cast("long"))
        df = df.withColumn("minimumOrderQuantity", col("minimumOrderQuantity").cast("long"))

        return df

    @log_method
    def filter_new_files(self, df):
        if DeltaTable.isDeltaTable(self.spark, self.output_path):
            already_loaded = self.spark.read.format("delta") \
                .load(self.output_path) \
                .select("source_file") \
                .distinct()
            df = df.join(already_loaded, on="source_file", how="left_anti")
            self.logger.info("Filtered to new files only")
        else:
            self.logger.info("First load - no filtering needed")
        return df

    @log_method
    def write_delta(self, df):
        if df.count() == 0:
            self.logger.info("No new files found - skipping write")
            return
        df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(self.output_path)
        self.logger.info("Write successful")

    @log_method
    def create_table(self):
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name}
            USING DELTA
            LOCATION '{self.output_path}'
        """)
        self.logger.info("Table ready")

    def run(self):
        self.logger.info("===== Bronze Pipeline Started =====")
        df = self.read_json()
        df = self.flatten_json(df)
        df = self.add_metadata(df)
        df = self.filter_new_files(df)
        self.write_delta(df)
        self.create_table()
        upload_logs_to_adls(self.log_file, "bronze")
        self.logger.info("===== Bronze Pipeline Completed =====")


# COMMAND ----------

bronze = BronzeLayer(
    spark,
    "abfss://input@retailstoragesejal.dfs.core.windows.net/Raw_data",
    "abfss://input@retailstoragesejal.dfs.core.windows.net/Bronze/delta/retail_table/",
    "retail_catalog.bronze.retail_table"
)

bronze.run()

# COMMAND ----------

df = spark.read.table("retail_catalog.bronze.retail_table")
display(df)

# COMMAND ----------

spark.table("retail_catalog.bronze.retail_table").count()

# COMMAND ----------

