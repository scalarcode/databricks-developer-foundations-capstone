# Databricks notebook source
# MAGIC %run ./Setup-Common

# COMMAND ----------

html = "<html><body><table>"
html += """<p style="font-size:16px">The following variables have been defined for you.<br/>Please refer to them in the following instructions."""
html += """<tr><th style="padding: 0 1em 0 0; text-align:left">Variable</th><th style="padding: 0 1em 0 0; text-align:left">Value</th><th style="padding: 0 1em 0 0; text-align:left">Description</th></tr>"""

html += html_row("username", username, """This is the email address that you signed into Databricks with. It is used here to create a "globally unique" working directory.""")
html += html_row("working_dir", working_dir, """This is directory in which all work should be conducted. This helps to ensure there are not conflicts with other users in the workspace.""")
html += html_row("batch_2017_path", batch_2017_path, """The path to the 2017 batch of orders.""")
html += html_row("batch_2018_path", batch_2018_path, """The path to the 2018 batch of orders.""")
html += html_row("batch_2019_path", batch_2019_path, """The path to the 2019 batch of orders.""")
html += html_row("batch_target_path", batch_target_path, """The location of the new, unified, raw, batch of orders & sales reps.""")

html += "</table></body></html>"

displayHTML(html)

# COMMAND ----------

def createExpectedSchema():
  from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
  return StructType([StructField("submitted_at", StringType(), True),
                     StructField("order_id", StringType(), True),
                     StructField("customer_id", StringType(), True),
                     StructField("sales_rep_id", StringType(), True),
                     StructField("sales_rep_ssn", StringType(), True),
                     StructField("sales_rep_first_name", StringType(), True),
                     StructField("sales_rep_last_name", StringType(), True),
                     StructField("sales_rep_address", StringType(), True),
                     StructField("sales_rep_city", StringType(), True),
                     StructField("sales_rep_state", StringType(), True),
                     StructField("sales_rep_zip", StringType(), True),
                     StructField("shipping_address_attention", StringType(), True),
                     StructField("shipping_address_address", StringType(), True),
                     StructField("shipping_address_city", StringType(), True),
                     StructField("shipping_address_state", StringType(), True),
                     StructField("shipping_address_zip", StringType(), True),
                     StructField("product_id", StringType(), True),
                     StructField("product_quantity", StringType(), True),
                     StructField("product_sold_price", StringType(), True),
                     StructField("ingest_file_name", StringType(), True),
                     StructField("ingested_at", TimestampType(), True)])
expectedSchema = createExpectedSchema()

expected_columns = list(map(lambda f: f.name, expectedSchema))

string_columns = expected_columns.copy()
string_columns.remove("ingested_at")

check_a_passed = False
check_b_passed = False
check_c_passed = False

# COMMAND ----------

def reality_check_02_A():
  global check_a_passed
  
  suite_name = "ex.02.a"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.exists", "Target directory exists",  
             testFunction = lambda: len(list(filter(lambda f: f.path.endswith("/"), dbutils.fs.ls( batch_target_path)))) > 0)
  
  suite.test(f"{suite_name}.is_delta", "Using the Delta file format", dependsOn=[suite.lastTestId],
             testFunction = lambda: len(list(filter(lambda f: f.path.endswith("/_delta_log/"), dbutils.fs.ls( batch_target_path)))) == 1)
  
  suite.test(f"{suite_name}.has_parquet", "Found at least one Parquet part-file", dependsOn=[suite.lastTestId],
             testFunction = lambda: len(list(filter(lambda f: f.path.endswith(".parquet"), dbutils.fs.ls( batch_target_path)))) > 0)
  
  expected = meta_batch_count_2017
  actual = spark.read.format("delta").load(batch_target_path).count()
  suite.test(f"{suite_name}.count", f"Expected {expected:,d} records, found {actual:,d}", dependsOn=[suite.lastTestId],
             testFunction = lambda: actual == expected)
  
  suite.test(f"{suite_name}.schema", "Schema is valid", dependsOn=[suite.lastTestId],
             testFunction = lambda: checkSchema(spark.read.format("delta").load(batch_target_path).schema, expectedSchema, False, False))
  
  daLogger.logEvent(f"{suite_name}", f"{{\"registration_id\": {registration_id}, \"passed\": {suite.passed}, \"percentage\": {suite.percentage}, \"actPoints\": {suite.score}, \"maxPoints\": {suite.maxScore}}}")
  
  check_a_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_02_B():
  global check_b_passed

  suite_name = "ex.02.b"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.exists", "Target directory exists",  
             testFunction = lambda: len(list(filter(lambda f: f.path.endswith("/"), dbutils.fs.ls( batch_target_path)))) > 0)
  
  suite.test(f"{suite_name}.is_delta", "Using the Delta file format", dependsOn=[suite.lastTestId],
             testFunction = lambda: len(list(filter(lambda f: f.path.endswith("/_delta_log/"), dbutils.fs.ls( batch_target_path)))) == 1)
  
  suite.test(f"{suite_name}.has_parquet", "Found at least one Parquet part-file", dependsOn=[suite.lastTestId],
             testFunction = lambda: len(list(filter(lambda f: f.path.endswith(".parquet"), dbutils.fs.ls( batch_target_path)))) > 0)
  
  expected = meta_batch_count_2017+meta_batch_count_2018
  actual = spark.read.format("delta").load(batch_target_path).count()
  suite.test(f"{suite_name}.count", f"Expected {expected:,d} records, found {actual:,d}", dependsOn=[suite.lastTestId],
             testFunction = lambda: actual == expected)
  
  suite.test(f"{suite_name}.schema", "Schema is valid", dependsOn=[suite.lastTestId],
             testFunction = lambda: checkSchema(spark.read.format("delta").load(batch_target_path).schema, expectedSchema, False, False))
  
  daLogger.logEvent(f"{suite_name}", f"{{\"registration_id\": {registration_id}, \"passed\": {suite.passed}, \"percentage\": {suite.percentage}, \"actPoints\": {suite.score}, \"maxPoints\": {suite.maxScore}}}")
  
  check_b_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_02_C():
  global check_c_passed

  suite_name = "ex.02.c"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.exists", "Target directory exists",  
             testFunction = lambda: len(list(filter(lambda f: f.path.endswith("/"), dbutils.fs.ls( batch_target_path)))) > 0)
  
  suite.test(f"{suite_name}.is_delta", "Using the Delta file format", dependsOn=[suite.lastTestId],
             testFunction = lambda: len(list(filter(lambda f: f.path.endswith("/_delta_log/"), dbutils.fs.ls( batch_target_path)))) == 1)
  
  suite.test(f"{suite_name}.has_parquet", "Found at least one Parquet part-file", dependsOn=[suite.lastTestId],
             testFunction = lambda: len(list(filter(lambda f: f.path.endswith(".parquet"), dbutils.fs.ls( batch_target_path)))) > 0)
  
  expected = meta_batch_count_2017+meta_batch_count_2018+meta_batch_count_2019
  actual = spark.read.format("delta").load(batch_target_path).count()
  suite.test(f"{suite_name}.count", f"Expected {expected:,d} records, found {actual:,d}", dependsOn=[suite.lastTestId],
             testFunction = lambda: actual == expected)
  
  suite.test(f"{suite_name}.schema", "Schema is valid", dependsOn=[suite.lastTestId],
             testFunction = lambda: checkSchema(spark.read.format("delta").load(batch_target_path).schema, expectedSchema, False, False))
  
  daLogger.logEvent(f"{suite_name}", f"{{\"registration_id\": {registration_id}, \"passed\": {suite.passed}, \"percentage\": {suite.percentage}, \"actPoints\": {suite.score}, \"maxPoints\": {suite.maxScore}}}")
  
  check_c_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def full_assessment_02():
  global check_a_passed
  global check_b_passed
  global check_c_passed
  
  from pyspark.sql.functions import year, month, dayofmonth, from_unixtime
  from datetime import datetime

  suite_name = "ex.02.all"
  suite = TestSuite()
  
  suite.testEquals(f"{suite_name}.a-passed", "Reality Check 02.A passed", check_a_passed, True)
  suite.testEquals(f"{suite_name}.b-passed", "Reality Check 02.B passed", check_b_passed, True, dependsOn=[suite.lastTestId])
  suite.testEquals(f"{suite_name}.c-passed", "Reality Check 02.C passed", check_c_passed, True, dependsOn=[suite.lastTestId])
  
  suite.test(f"{suite_name}.exists", "Target directory exists", dependsOn=[suite.lastTestId],  
             testFunction = lambda: len(list(filter(lambda f: f.path.endswith("/"), dbutils.fs.ls( batch_target_path)))) > 0)
  
  suite.test(f"{suite_name}.is_delta", "Using the Delta file format", dependsOn=[suite.lastTestId],
             testFunction = lambda: len(list(filter(lambda f: f.path.endswith("/_delta_log/"), dbutils.fs.ls( batch_target_path)))) == 1)
  
  suite.test(f"{suite_name}.has_parquet", "Found at least one Parquet part-file", dependsOn=[suite.lastTestId],
             testFunction = lambda: len(list(filter(lambda f: f.path.endswith(".parquet"), dbutils.fs.ls( batch_target_path)))) > 0)

  expected = meta_batch_count_2017+meta_batch_count_2018+meta_batch_count_2019
  actual = spark.read.format("delta").load(batch_target_path).count()
  suite.test(f"{suite_name}.count", f"Expected {expected:,d} records, found{actual:,d}", dependsOn=[suite.lastTestId],
             testFunction = lambda: actual == expected)
  
  suite.test(f"{suite_name}.schema", "Schema is valid", dependsOn=[suite.lastTestId],
             testFunction = lambda: checkSchema(spark.read.format("delta").load(batch_target_path).schema, expectedSchema, False, False))

  def no_white_space():
    for column in string_columns:
      if 0 != spark.read.format("delta").load(batch_target_path).filter(col(column) != trim(col(column))).count():
        return False
    return True

  suite.test(f"{suite_name}.white-space", "No whitespace in column values, need to trim whitespace", testFunction=no_white_space, dependsOn=[suite.lastTestId])
  
  def no_empty_strings():
    for column in string_columns:
      if 0 != spark.read.format("delta").load(batch_target_path).filter(trim(col(column)) == lit("")).count():
        return False
    return True
    
  suite.test(f"{suite_name}.empty-strings", "No empty strings in column values, should be null literals", testFunction=no_empty_strings, dependsOn=[suite.lastTestId])

  def no_null_strings():
    for column in string_columns:
      if 0 != spark.read.format("delta").load(batch_target_path).filter(trim(col(column)) == lit("null")).count():
        return False
    return True
    
  suite.test(f"{suite_name}.null-strings", "No null strings, should be null literals", testFunction=no_null_strings, dependsOn=[suite.lastTestId])
    
  def valid_ingest_file_name():
    for expectedYear, expectedExt in [(2017,"txt"),(2018,"csv"),(2019,"csv")]:
      tempDF = spark.read.format("delta").load(batch_target_path).withColumn("submitted_at", from_unixtime("submitted_at").cast("timestamp"))
      if 0 != tempDF.filter(year(col("submitted_at")) == expectedYear).filter(col("ingest_file_name").endswith(f"{username}/dbacademy/INT-ASPCP-v1-SP/raw/orders/batch/{expectedYear}.{expectedExt}") == False).count():
        return False
    return True

  suite.test(f"{suite_name}.ingest-file", "Ingest file names are valid", testFunction=valid_ingest_file_name, dependsOn=[schema_id])

  def valid_ingest_date():
    today = datetime.today()
    actual = spark.read.format("delta").load(batch_target_path).filter(year("ingested_at") == today.year).filter(month("ingested_at") == today.month).filter(dayofmonth("ingested_at") == today.day).count()
    return actual == meta_batch_count_2017+meta_batch_count_2018+meta_batch_count_2019
    
  suite.test(f"{suite_name}.ingest-date", "Ingest date is valid", testFunction=valid_ingest_date, dependsOn=[schema_id])
    
  daLogger.logEvent(f"{suite_name}", f"{{\"registration_id\": {registration_id}, \"passed\": {suite.passed}, \"percentage\": {suite.percentage}, \"actPoints\": {suite.score}, \"maxPoints\": {suite.maxScore}}}")
  
  daLogger.logEvent(f"ex.02.final", f"{{\"registration_id\": {registration_id}, \"passed\": {TestResultsAggregator.passed}, \"percentage\": {TestResultsAggregator.percentage}, \"actPoints\": {TestResultsAggregator.score}, \"maxPoints\":   {TestResultsAggregator.maxScore}}}")
  
  suite.displayResults()