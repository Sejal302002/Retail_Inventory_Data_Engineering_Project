# Databricks notebook source
# MAGIC %md
# MAGIC ## External Location

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION `retail-exc`
# MAGIC URL 'abfss://input@retailstoragesejal.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `retail-credential`);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG retail_catalog
# MAGIC MANAGED LOCATION 'abfss://input@retailstoragesejal.dfs.core.windows.net/catalog/';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA retail_catalog.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA retail_catalog.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA retail_catalog.gold;

# COMMAND ----------

