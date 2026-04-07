# Databricks notebook source
import logging
import os
from datetime import datetime

def get_logger(name,layer):

    log_dir = f"/databricks/driver/logs/{layer}"
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, f"{name}.log")

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    file_handler = logging.FileHandler(log_file, mode='a')
    console_handler = logging.StreamHandler()

    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.propagate = False

    return logger, log_file 

    

# COMMAND ----------

