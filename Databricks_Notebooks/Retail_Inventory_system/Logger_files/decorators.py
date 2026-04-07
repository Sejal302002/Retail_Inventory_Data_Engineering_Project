# Databricks notebook source

import functools
import time

def log_method(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        self.logger.info(f"Started  : {func.__name__}")
        start = time.time()
        try:
            result = func(self, *args, **kwargs)
            duration = round(time.time() - start, 2)
            self.logger.info(f"Completed: {func.__name__} in {duration}s")
            return result
        except Exception as e:
            self.logger.error(f"Failed   : {func.__name__} → {str(e)}")
            raise
    return wrapper

# COMMAND ----------

