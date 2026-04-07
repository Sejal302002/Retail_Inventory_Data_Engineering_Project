# Databricks notebook source
def upload_logs_to_adls(log_file, layer_name):
    try:
        file_name = log_file.split("/")[-1]

        source = f"file:{log_file}"

        target = f"abfss://input@retailstoragesejal.dfs.core.windows.net/logs/{layer_name}/{file_name}"

        dbutils.fs.cp(source, target, True)

        print(f" {layer_name} logs uploaded to ADLS")

    except Exception as e:
        print(f" Log upload failed: {str(e)}")

# COMMAND ----------

