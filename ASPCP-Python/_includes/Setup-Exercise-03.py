# Databricks notebook source
# MAGIC %run ./Setup-Common

# COMMAND ----------

html = "<html><body><table>"
html += """<p style="font-size:16px">The following variables have been defined for you.<br/>Please refer to them in the following instructions."""
html += """<tr><th style="padding: 0 1em 0 0; text-align:left">Variable</th><th style="padding: 0 1em 0 0; text-align:left">Value</th><th style="padding: 0 1em 0 0; text-align:left">Description</th></tr>"""

html += html_row("username", username, """This is the email address that you signed into Databricks with. It is used here to create a "globally unique" working directory.""")
html += html_row("working_dir", working_dir, """This is directory in which all work should be conducted. This helps to ensure there are not conflicts with other users in the workspace.""")
html += html_row("batch_source_path", batch_target_path, """The location of the combined, raw, batch of orders.""")
html += html_row("user_db", user_db, """The name of the database you will use for this project.""")
html += html_row("orders_table", orders_table, """The name of the orders table.""")
html += html_row("line_items_table", line_items_table, """The name of the line items table.""")
html += html_row("sales_reps_table", sales_reps_table, """The name of the sales reps table.""")

html += html_row("batch_temp_view", batch_temp_view, """The name of the temp view used in this exercise""")

html += "</table></body></html>"

displayHTML(html)

# COMMAND ----------

def createExpectedSalesRepSchema():
  from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType, IntegerType, TimestampType, DoubleType, BooleanType
  return StructType([StructField("sales_rep_id", StringType(), True),
                     StructField("sales_rep_ssn", LongType(), True),
                     StructField("sales_rep_first_name", StringType(), True),
                     StructField("sales_rep_last_name", StringType(), True),
                     StructField("sales_rep_address", StringType(), True),
                     StructField("sales_rep_city", StringType(), True),
                     StructField("sales_rep_state", StringType(), True),
                     StructField("sales_rep_zip", IntegerType(), True),
                     StructField("ingest_file_name", StringType(), True),
                     StructField("ingested_at", TimestampType(), True),
                     StructField("_error_ssn_format", BooleanType(), True),
                    ])
expectedSalesRepSchema = createExpectedSalesRepSchema()

def createExpectedOrdersSchema():
  from pyspark.sql.types import StructType, StructField, ArrayType, DecimalType, StringType, DateType, LongType, IntegerType, TimestampType, DoubleType, BooleanType
  return StructType([StructField("submitted_at", TimestampType(), True),
                     StructField("submitted_yyyy_mm", StringType(), True),
                     StructField("order_id", StringType(), True),
                     StructField("customer_id", StringType(), True),
                     StructField("sales_rep_id", StringType(), True),
                     
                     StructField("shipping_address_attention", StringType(), True),
                     StructField("shipping_address_address", StringType(), True),
                     StructField("shipping_address_city", StringType(), True),
                     StructField("shipping_address_state", StringType(), True),
                     StructField("shipping_address_zip", IntegerType(), True),

                     StructField("ingest_file_name", StringType(), True),
                     StructField("ingested_at", TimestampType(), True),
                    ])
expectedOrdersSchema = createExpectedOrdersSchema()

def createLineItemSchema():
  from pyspark.sql.types import StructType, StructField, ArrayType, DecimalType, StringType, DateType, LongType, IntegerType, TimestampType, DoubleType, BooleanType
  return StructType([StructField("order_id", StringType(), True),
                     StructField("product_id", StringType(), True),
                     
                     StructField("product_quantity", IntegerType(), True),
                     StructField("product_sold_price", DecimalType(10,2), True),

                     StructField("ingest_file_name", StringType(), True),
                     StructField("ingested_at", TimestampType(), True),
                    ])
expectedLineItemsSchema = createLineItemSchema()

check_a_passed = False
check_b_passed = False
check_c_passed = False
check_d_passed = False
check_e_passed = False
check_final_passed = False

# COMMAND ----------

def reality_check_03_A():
  global check_a_passed
  
  suite_name = "ex.03.a"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.current-db", f"The current database is {user_db}",  
             testFunction = lambda: spark.catalog.currentDatabase() == user_db)

  daLogger.logEvent(f"{suite_name}", f"{{\"registration_id\": {registration_id}, \"passed\": {suite.passed}, \"percentage\": {suite.percentage}, \"actPoints\": {suite.score}, \"maxPoints\": {suite.maxScore}}}")
  
  check_a_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_03_B():
  global check_b_passed

  suite_name = "ex.03.b"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.table-exists", f"The table {batch_temp_view} exists",  
             testFunction = lambda: len(list(filter(lambda t: t.name==batch_temp_view, spark.catalog.listTables(user_db)))) == 1)
  
  suite.test(f"{suite_name}.is-temp-view", f"The table {batch_temp_view} is a temp view", dependsOn=[suite.lastTestId()],
             testFunction = lambda: list(filter(lambda t: t.name==batch_temp_view, spark.catalog.listTables(user_db)))[0].tableType == "TEMPORARY")
  
  suite.test(f"{suite_name}.is-cached", f"The table {batch_temp_view} is cached", dependsOn=[suite.lastTestId()],
             testFunction = lambda: spark.catalog.isCached(batch_temp_view))

  expected = meta_batch_count_2017+meta_batch_count_2018+meta_batch_count_2019
  actual = spark.read.table(batch_temp_view).count()
  suite.test(f"{suite_name}.count", f"Expected {expected:,d} records, found {actual:,d}", dependsOn=[suite.lastTestId()],
             testFunction = lambda:  actual == expected)
  
  daLogger.logEvent(f"{suite_name}", f"{{\"registration_id\": {registration_id}, \"passed\": {suite.passed}, \"percentage\": {suite.percentage}, \"actPoints\": {suite.score}, \"maxPoints\": {suite.maxScore}}}")
  
  check_b_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_03_C():
  global check_c_passed

  suite_name = "ex.03.c"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.table-exists", f"The table {sales_reps_table} exists",  
             testFunction = lambda: len(list(filter(lambda t: t.name==sales_reps_table, spark.catalog.listTables(user_db)))) == 1)
  
  suite.test(f"{suite_name}.is-managed", f"The table {sales_reps_table} is a managed table", dependsOn=[suite.lastTestId()],
             testFunction = lambda: list(filter(lambda t: t.name==sales_reps_table, spark.catalog.listTables(user_db)))[0].tableType == "MANAGED")

  hive_path = f"dbfs:/user/hive/warehouse/{user_db}.db/{sales_reps_table}"
  suite.test(f"{suite_name}.is_delta", "Using the Delta file format", dependsOn=[suite.lastTestId()],
           testFunction = lambda: len(list(filter(lambda f: f.path.endswith("/_delta_log/"), dbutils.fs.ls( hive_path)))) == 1)

  suite.test(f"{suite_name}.schema", "Schema is valid", dependsOn=[suite.lastTestId()],
             testFunction = lambda: checkSchema(spark.read.table(sales_reps_table).schema, expectedSalesRepSchema, False, False))

  actual = spark.read.table(sales_reps_table).count()
  suite.testEquals(f"{suite_name}.count-total", f"Expected {meta_sales_reps_count:,d} records, found {actual:,d}", 
             actual, meta_sales_reps_count, dependsOn=[suite.lastTestId()])

  actual = spark.read.table(sales_reps_table).filter("_error_ssn_format == true").count()
  suite.testEquals(f"{suite_name}.count-ssn-format", f"Expected _error_ssn_format record count to be {meta_ssn_format_count:,d}, found {actual:,d}", 
             actual, meta_ssn_format_count, dependsOn=[suite.lastTestId()])

  daLogger.logEvent(f"{suite_name}", f"{{\"registration_id\": {registration_id}, \"passed\": {suite.passed}, \"percentage\": {suite.percentage}, \"actPoints\": {suite.score}, \"maxPoints\": {suite.maxScore}}}")
  
  check_c_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_03_D():
  global check_d_passed

  suite_name = "ex.03.d"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.table-exists", f"The table {orders_table} exists",  
             testFunction = lambda: len(list(filter(lambda t: t.name==orders_table, spark.catalog.listTables(user_db)))) == 1)
  
  suite.test(f"{suite_name}.is-managed", f"The table {orders_table} is a managed table", dependsOn=[suite.lastTestId()],
             testFunction = lambda: list(filter(lambda t: t.name==orders_table, spark.catalog.listTables(user_db)))[0].tableType == "MANAGED")

  hive_path = f"dbfs:/user/hive/warehouse/{user_db}.db/{orders_table}"
  suite.test(f"{suite_name}.is_delta", "Using the Delta file format", dependsOn=[suite.lastTestId()],
           testFunction = lambda: len(list(filter(lambda f: f.path.endswith("/_delta_log/"), dbutils.fs.ls( hive_path)))) == 1)
  
  suite.test(f"{suite_name}.schema", "Schema is valid", dependsOn=[suite.lastTestId()],
             testFunction = lambda: checkSchema(spark.read.table(orders_table).schema, expectedOrdersSchema, False, False))

  actual = spark.read.table(orders_table).count()
  suite.test(f"{suite_name}.count-total", f"Expected {meta_orders_count:,d} records, found {actual:,d}", dependsOn=[suite.lastTestId()],
             testFunction = lambda: actual == meta_orders_count)

  suite.test(f"{suite_name}.non-null-submitted_at", f"Non-null (properly parsed) submitted_at", dependsOn=[suite.lastTestId()],
             testFunction = lambda: spark.read.table(orders_table).filter(col("submitted_at").isNull()).count() == 0)

  def is_partitioned():
    files = filter(lambda p: p.endswith("_delta_log/") == False, map(lambda f: f.path, dbutils.fs.ls(hive_path)))
    
    for y in range(2017, 2020):
      for m in ["01","02","03","04","05","06","07","08","09","10","11","12"]:
        path = f"{hive_path}/submitted_yyyy_mm={y}-{m}"
        if path in files == False:
          return False
    return True
  suite.test(f"{suite_name}.is_partitioned", f"Partitioned by submitted_yyyy_mm", dependsOn=[suite.lastTestId()], testFunction = is_partitioned)
  
  daLogger.logEvent(f"{suite_name}", f"{{\"registration_id\": {registration_id}, \"passed\": {suite.passed}, \"percentage\": {suite.percentage}, \"actPoints\": {suite.score}, \"maxPoints\": {suite.maxScore}}}")
  
  check_d_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_03_E():
  global check_e_passed

  suite_name = "ex.03.e"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.table-exists", f"The table {line_items_table} exists",  
             testFunction = lambda: len(list(filter(lambda t: t.name==line_items_table, spark.catalog.listTables(user_db)))) == 1)
  
  suite.test(f"{suite_name}.is-managed", f"The table {line_items_table} is a managed table", dependsOn=[suite.lastTestId()],
             testFunction = lambda: list(filter(lambda t: t.name==line_items_table, spark.catalog.listTables(user_db)))[0].tableType == "MANAGED")

  hive_path = f"dbfs:/user/hive/warehouse/{user_db}.db/{line_items_table}"
  suite.test(f"{suite_name}.is_delta", "Using the Delta file format", dependsOn=[suite.lastTestId()],
           testFunction = lambda: len(list(filter(lambda f: f.path.endswith("/_delta_log/"), dbutils.fs.ls( hive_path)))) == 1)
  
  suite.test(f"{suite_name}.schema", "Schema is valid", dependsOn=[suite.lastTestId()],
             testFunction = lambda: checkSchema(spark.read.table(line_items_table).schema, expectedLineItemsSchema, False, False))

  actual = spark.read.table(line_items_table).count()
  suite.test(f"{suite_name}.count-total", f"Expected {meta_line_items_count:,d} records, found {actual:,d}", dependsOn=[suite.lastTestId()],
             testFunction = lambda: actual == meta_line_items_count)

  daLogger.logEvent(f"{suite_name}", f"{{\"registration_id\": {registration_id}, \"passed\": {suite.passed}, \"percentage\": {suite.percentage}, \"actPoints\": {suite.score}, \"maxPoints\": {suite.maxScore}}}")
  
  check_e_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def full_assessment_03():
  global check_final_passed
  from pyspark.sql.functions import col

  suite_name = "ex.03.all"
  suite = TestSuite()
  
  suite.testEquals(f"{suite_name}.a-passed", "Reality Check 03.A passed", check_a_passed, True)
  suite.testEquals(f"{suite_name}.b-passed", "Reality Check 03.B passed", check_b_passed, True, dependsOn=[suite.lastTestId()])
  suite.testEquals(f"{suite_name}.c-passed", "Reality Check 03.C passed", check_c_passed, True, dependsOn=[suite.lastTestId()])
  suite.testEquals(f"{suite_name}.d-passed", "Reality Check 03.D passed", check_d_passed, True, dependsOn=[suite.lastTestId()])
  suite.testEquals(f"{suite_name}.e-passed", "Reality Check 03.E passed", check_e_passed, True, dependsOn=[suite.lastTestId()])
    
  check_final_passed = suite.passed
  
  daLogger.logEvent(f"{suite_name}", f"{{\"registration_id\": {registration_id}, \"passed\": {suite.passed}, \"percentage\": {suite.percentage}, \"actPoints\": {suite.score}, \"maxPoints\": {suite.maxScore}}}")
  
  daLogger.logEvent(f"ex.02.final", f"{{\"registration_id\": {registration_id}, \"passed\": {TestResultsAggregator.passed}, \"percentage\": {TestResultsAggregator.percentage}, \"actPoints\": {TestResultsAggregator.score}, \"maxPoints\":   {TestResultsAggregator.maxScore}}}")
  
  suite.displayResults()