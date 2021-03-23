# Databricks notebook source
skip_meta = True

# COMMAND ----------

# MAGIC %run ./Setup-Common

# COMMAND ----------

# The user's name (email address) will be used to create a home directory into which all datasets will
# be written into so as to further isolating changes from other students running the same material.
username = getTag("user")
working_dir = f"dbfs:/user/{username}/dbacademy/{dataset_name}"
raw_data_dir = f"{working_dir}/raw"

def install_datasets():
  source_dir = f"wasbs://courseware@dbacademy.blob.core.windows.net/{dataset_name}/v01"
  print(f"\nThe source directory for this dataset is\n{source_dir}/\n")

  # Remove old versions of the previously installed datasets
  print(f"Removing previously installed datasets from\n{raw_data_dir}/")
  deleted = dbutils.fs.rm(raw_data_dir, True)
  print("Done - removed the previously installed datasets" if deleted else "Done - no datasets found to remove")

  # Install the orders datasets
  print(f"""\nInstalling the datasets to {raw_data_dir}""")
  
  print(f"""\nNOTE: The datasets that we are installing are located in Washington, USA - depending on the
      region that your workspace is in, this operation can take as little as 30 seconds and 
      upwards to 5 minutes, but this is a one-time operation.""")

  dbutils.fs.rm(raw_data_dir, True)
  dbutils.fs.cp(source_dir, raw_data_dir, True)
  print(f"""\nThe install of the datasets completed successfully.""")  

def reality_check_install():
  load_meta()
  
  suite_name = "install"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.cluster", f"Using DBR 7.3 LTS", testFunction = validate_cluster)
  cluster_id = suite.lastTestId()
  
  suite.test(f"{suite_name}.reg_id", f"Valid Registration ID", testFunction = lambda: validate_registration_id(registration_id), dependsOn=cluster_id)
  reg_id_id = suite.lastTestId()
  
  test_1_count = len(dbutils.fs.ls(raw_data_dir+"/"))
  suite.testEquals(f"{suite_name}.root", f"Expected 3 files, found {test_1_count} in /", 3, test_1_count, dependsOn=reg_id_id)
  test_1_id = suite.lastTestId()
  
  test_2_count = len(dbutils.fs.ls(raw_data_dir+"/_meta"))
  suite.testEquals(f"{suite_name}.meta", f"Expected 1 files, found {test_2_count} in /_meta", 1, test_2_count, dependsOn=reg_id_id)
  test_2_id = suite.lastTestId()
  
  test_3_count = len(dbutils.fs.ls(raw_data_dir+"/products"))
  suite.testEquals(f"{suite_name}.products", f"Expected 2 files, found {test_3_count} in /products", 2, test_3_count, dependsOn=reg_id_id)
  test_3_id = suite.lastTestId()
  
  test_4_count = len(dbutils.fs.ls(raw_data_dir+"/orders"))
  suite.testEquals(f"{suite_name}.orders", f"Expected 2 files, found {test_4_count} in /orders", 2, test_4_count, dependsOn=reg_id_id)
  test_4_id = suite.lastTestId()
  
  test_5_count = len(dbutils.fs.ls(raw_data_dir+"/orders/batch"))
  suite.testEquals(f"{suite_name}.orders-batch", f"Expected 3 files, found {test_5_count} in /orders/batch", 3, test_5_count, dependsOn=reg_id_id)
  test_5_id = suite.lastTestId()
  
  test_6_count = len(dbutils.fs.ls(raw_data_dir+"/orders/stream"))
  stream_count = spark.read.option("inferSchema", True).json(f"{raw_data_dir}/_meta/meta.json").first()["streamCount"]
  suite.testEquals(f"{suite_name}.orders-stream", f"Expected {stream_count} files, found {test_6_count} in /orders/stream", stream_count, test_6_count, dependsOn=reg_id_id)
  test_6_id = suite.lastTestId()

  previous_ids = [test_1_id, test_2_id, test_3_id, test_4_id, test_5_id, test_6_id]
  suite.test(f"{suite_name}.all", "All datasets were installed succesfully!", dependsOn=previous_ids,
             testFunction = lambda: True)
  
  daLogger.logEvent(f"Test-{suite_name}.suite", f"""{{
    "registrationId": "{registration_id}", 
    "testId": "Suite-{suite_name}", 
    "description": "Suite level results",
    "status": "{"passed" if suite.passed else "failed"}",
    "actPoints": "{suite.score}", 
    "maxPoints": "{suite.maxScore}",
    "percentage": "{suite.percentage}"
  }}""")
  
  daLogger.logEvent(f"Lesson.final", f"""{{
    "registrationId": "{registration_id}", 
    "testId": "Aggregated-{getLessonName()}", 
    "description": "Aggregated results for lesson",
    "status": "{"passed" if TestResultsAggregator.passed else "failed"}",
    "actPoints": "{TestResultsAggregator.score}", 
    "maxPoints":   "{TestResultsAggregator.maxScore}",
    "percentage": "{TestResultsAggregator.percentage}"
  }}""")

  suite.displayResults()  
  
None # Suppress output

# COMMAND ----------

html = html_intro()
html += html_header()

html += html_row_fun("install_datasets()", "A utility function for installing datasets into the current workspace.")
html += html_row_fun("reality_check_install()", "A utility function for validating the install process.")

html += "</table></body></html>"

displayHTML(html)
