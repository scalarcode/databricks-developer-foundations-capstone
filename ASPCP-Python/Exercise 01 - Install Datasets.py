# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md # Exercise #1 - Download Datasets
# MAGIC 
# MAGIC The datasets for this project are stored in a public object store.
# MAGIC 
# MAGIC They need to be downloaded and installed into your Databricks workspace before proceeding with this project.
# MAGIC 
# MAGIC To complete this operation, 
# MAGIC 1. Attach this notebook to a cluster
# MAGIC 2. Select **Run All** from above
# MAGIC 3. Go to the end of the notebook and review the results - looking for confirmation of success
# MAGIC 
# MAGIC This entire processes should take as little as 3 minutes but can take significantly longer depending on which region your workspace is in.
# MAGIC 
# MAGIC **NOTE:** It is not necceissary to fully understand the following code - just to run it.</br>
# MAGIC But for those of you that are interested in what this code is doing...
# MAGIC * **Cmd 3**: Declare key variables and removes previously installed datsets.
# MAGIC * **Cmd 4**: Installs the various dataset.
# MAGIC * **Cmd 5**: Validates that all the datasets were installed correctly.

# COMMAND ----------

# The user's name (email address) will be used to create a home directory into which all datasets will
# be written into so as to further isolating changes from other students running the same material.
username = sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(dbutils.entry_point.getDbutils().notebook().getContext().tags())["user"]
print(f"""Your username is\n{username}""")

# This course's unique identifer
dbacademy_sku = "INT-ASPCP-v1"

working_dir = f"dbfs:/user/{username}/dbacademy/{dbacademy_sku}"
print(f"""\nYour working directory is\n{working_dir}""")

source_dir = f"wasbs://courseware@dbacademy.blob.core.windows.net/{dbacademy_sku}/v01"
print(f"\nThe source directory for this dataset is\n{source_dir}/\n")

# Remove old versions of the previously installed datasets
raw_data_dir = f"{working_dir}/raw"
print(f"Removing previously installed datasets from\n{raw_data_dir}/")
deleted = dbutils.fs.rm(raw_data_dir, True)
print("Done - removed the previously installed datasets" if deleted else "Done - no datasets found to remove")

# COMMAND ----------

# Install the orders datasets
print(f"""Installing the datasets to {raw_data_dir}""")
print(f"NOTE: Depending on the region that your workspace is in, this can take 1-3 minutes or more.")

dbutils.fs.rm(raw_data_dir, True)
dbutils.fs.cp(source_dir, raw_data_dir, True)
print(f"""\nThe install of the datasets completed successfully.""")

# COMMAND ----------

# Declare a simple utility method to help validate that the datasets were installed correctly
def validate_file_count(path, expected):
  final_path = raw_data_dir+path
  files_count = len(dbutils.fs.ls(final_path))
  assert files_count == expected, f"Expected {expected} files but found {files_count} in {final_path}"
  print(f"Tests passed for ...{final_path}, found {expected} file{'s' if files_count != 1 else ''}")
  
# Validate each dataset
validate_file_count("/", 3)
validate_file_count("/_meta", 1)
validate_file_count("/products", 2)
validate_file_count("/orders", 2)
validate_file_count("/orders/batch", 3)
validate_file_count("/orders/stream", spark.read.option("inferSchema", True).json(f"{raw_data_dir}/_meta/meta.json").first()["streamCount"])

print("\nAll datasets were installed succesfully!")