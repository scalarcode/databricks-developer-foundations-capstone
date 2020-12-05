# Databricks notebook source
# MAGIC %run ./Setup-Common

# COMMAND ----------

html = "<html><body><table>"
html += """<p style="font-size:16px">The following variables have been defined for you.<br/>Please refer to them in the following instructions."""
html += """<tr><th style="padding: 0 1em 0 0; text-align:left">Variable</th><th style="padding: 0 1em 0 0; text-align:left">Value</th><th style="padding: 0 1em 0 0; text-align:left">Description</th></tr>"""

html += html_row("username", username, """This is the email address that you signed into Databricks with. It is used here to create a "globally unique" working directory.""")
html += html_row("working_dir", working_dir, """This is directory in which all work should be conducted. This helps to ensure there are not conflicts with other users in the workspace.""")
html += html_row("user_db", user_db, """The name of the database you will use for this project.""")

html += html_row("orders_table", orders_table, """The name of the orders table.""")
html += html_row("products_table", products_table, """The name of the products table.""")
html += html_row("line_items_table", line_items_table, """The name of the line items table.""")

html += html_row("stream_path", stream_path, """The path to the stream directory of JSON orders""")
html += html_row("orders_checkpoint_path", orders_checkpoint_path, """The location of the checkpoint for streamed orders""")
html += html_row("line_items_checkpoint_path", line_items_checkpoint_path, """The location of the checkpoint for streamed orders""")

html += "</table></body></html>"

displayHTML(html)

# COMMAND ----------

def wait_for_stream_start(name, max_count):
  import time
  while len(list(filter(lambda query: query.name == name, spark.streams.active))) == 0:
    print(f"""Waiting for the stream "{name}" to start...""")
    time.sleep(5) # Give it a couple of seconds

  query = list(filter(lambda query: query.name == name, spark.streams.active))[0]
  print(f"""The stream "{name}" has started.""")
      
  while len(query.recentProgress) == 0:
    print(f"""The stream hasn't processed any trigger yet...""")
    time.sleep(5) # Give it a couple of seconds
    
  if len(query.recentProgress) == 1: print(f"""The stream has processed {len(query.recentProgress)} triggers so far.""")
  else: print(f"""The stream has processed {len(query.recentProgress)} triggers so far.""")

  while len(query.recentProgress) < max_count:
    print(f"Processing trigger {len(query.recentProgress)+1} of {max_count}...")
    if query.recentProgress[-1]["numInputRows"] > 1:
      raise Exception(f"Expected 1 record per trigger, found {query.recentProgress[-1]['numInputRows']}, aborting all tests.")
    time.sleep(5) # Give it a couple of seconds
  
  return query

def first_n_equal_one(name):
  query = list(filter(lambda query: query.name == name, spark.streams.active))[0]
  for i in range(0, meta_stream_count):
    if query.recentProgress[i]["numInputRows"] != 1:
      return False
  return True

check_a_passed = True
check_b_passed = True
check_c_passed = True
check_d_passed = True

# COMMAND ----------

def reality_check_05_A():
  global check_a_passed
  
  suite_name = "ex.05.a"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.current-db", f"The current database is {user_db}",  
             testFunction = lambda: spark.catalog.currentDatabase() == user_db)

  actual_order_count = spark.read.table(orders_table).count()
  suite.test(f"{suite_name}.o-total", f"Expected {meta_orders_count:,d} orders, found {actual_order_count:,d}", dependsOn=[suite.lastTestId],
             testFunction = lambda: actual_order_count == meta_orders_count)

  actual_li_count = spark.read.table(line_items_table).count()
  suite.test(f"{suite_name}.li-total", f"Expected {meta_line_items_count:,d} line-items, found {actual_li_count:,d}", dependsOn=[suite.lastTestId],
             testFunction = lambda: actual_li_count == meta_line_items_count)
  
  actual_products_count = spark.read.table(products_table).count()
  suite.testEquals(f"{suite_name}.p-count", f"Expected {meta_products_count} products, found {actual_products_count}", 
                   actual_products_count, meta_products_count, dependsOn=[suite.lastTestId])

  daLogger.logEvent(f"{suite_name}", f"{{\"registration_id\": {registration_id}, \"passed\": {suite.passed}, \"percentage\": {suite.percentage}, \"actPoints\": {suite.score}, \"maxPoints\": {suite.maxScore}}}")
  
  check_a_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_05_B():
  global check_b_passed
  
  suite_name = "ex.05.b"
  suite = TestSuite()
  
  oneRecordEach = True
  query = wait_for_stream_start(orders_table, meta_stream_count)
  print("Processing results...")

  actual = len(query.recentProgress)
  suite.testEquals(f"{suite_name}.min-count", f"Expected at least {meta_stream_count} triggers, found {actual}", actual >= meta_stream_count, True)
  suite.testEquals(f"{suite_name}.max-count", f"Expected less than 100 triggers, found {actual}", actual < 100, True, dependsOn=[suite.lastTestId])

  suite.test(f"{suite_name}.whatever", f"Expected the first {meta_stream_count} triggers to processes 1 record per trigger", dependsOn=[suite.lastTestId], 
             testFunction = lambda: first_n_equal_one(orders_table))
  
  suite.test(f"{suite_name}.exists", "Checkpoint directory exists", dependsOn=[suite.lastTestId],
           testFunction = lambda: len(dbutils.fs.ls(f"{orders_checkpoint_path}/metadata")) > 0)

  actual = spark.read.table(orders_table).count()
  expected = meta_orders_count + meta_stream_count
  suite.testEquals(f"{suite_name}.order_total", f"Expected {expected:,d} orders, found {actual:,d} ({meta_stream_count} new)", 
                   actual, expected, dependsOn=[suite.lastTestId])
  
  daLogger.logEvent(f"{suite_name}", f"{{\"registration_id\": {registration_id}, \"passed\": {suite.passed}, \"percentage\": {suite.percentage}, \"actPoints\": {suite.score}, \"maxPoints\": {suite.maxScore}}}")
  
  print("Stopping the stream...")
  query.stop()

  check_b_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_05_C():
  global check_c_passed
  
  suite_name = "ex.05.c"
  suite = TestSuite()
  
  query = wait_for_stream_start(line_items_table, meta_stream_count)
  print("Processing results...")

  actual_trigger_count = len(query.recentProgress)
  suite.testEquals(f"{suite_name}.min-count", f"Expected at least {meta_stream_count:,d} triggers, found {actual_trigger_count:,d}", actual_trigger_count >= meta_stream_count, True)
  suite.testEquals(f"{suite_name}.max-count", f"Expected less than 100 triggers, found {actual_trigger_count:,d}", actual_trigger_count < 100, True, dependsOn=[suite.lastTestId])

  suite.test(f"{suite_name}.whatever", f"Expected the first {meta_stream_count:,d} triggers to processes 1 record per trigger", dependsOn=[suite.lastTestId], testFunction = lambda: first_n_equal_one(line_items_table))

  suite.test(f"{suite_name}.exists", "Checkpoint directory exists", dependsOn=[suite.lastTestId],
           testFunction = lambda: len(dbutils.fs.ls(f"{line_items_checkpoint_path}/metadata")) > 0)

  actual_li_count = spark.read.table(line_items_table).count()
  new_count = spark.read.json(stream_path).select("orderId", explode("products")).count()
  expected_li_count = meta_line_items_count + new_count
  suite.testEquals(f"{suite_name}.li_total", f"Expected {expected_li_count:,d} records, found {actual_li_count:,d} ({new_count} new)", actual_li_count, expected_li_count, dependsOn=[suite.lastTestId])
  
  daLogger.logEvent(f"{suite_name}", f"{{\"registration_id\": {registration_id}, \"passed\": {suite.passed}, \"percentage\": {suite.percentage}, \"actPoints\": {suite.score}, \"maxPoints\": {suite.maxScore}}}")
  
  print("Stopping the stream...")
  query.stop()

  check_c_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def full_assessment_05():

  suite_name = "ex.05.all"
  suite = TestSuite()
  
  suite.testEquals(f"{suite_name}.a-passed", "Reality Check 05.A passed", check_a_passed, True)
  suite.testEquals(f"{suite_name}.b-passed", "Reality Check 05.B passed", check_b_passed, True, dependsOn=[suite.lastTestId])
  suite.testEquals(f"{suite_name}.c-passed", "Reality Check 05.C passed", check_c_passed, True, dependsOn=[suite.lastTestId])

  actual_order_count = spark.read.table(orders_table).count()
  expected_order_count = meta_orders_count + meta_stream_count
  suite.testEquals(f"{suite_name}.order_total", f"Expected {expected_order_count:,d} orders, found {actual_order_count:,d} ({meta_stream_count} new)", 
                   actual_order_count, expected_order_count, dependsOn=[suite.lastTestId])

  actual_li_count = spark.read.table(line_items_table).count()
  new_count = spark.read.json(stream_path).select("orderId", explode("products")).count()
  expected_li_count = meta_line_items_count + new_count
  suite.testEquals(f"{suite_name}.li_total", f"Expected {expected_li_count:,d} records, found {actual_li_count:,d} ({new_count} new)", actual_li_count, expected_li_count, dependsOn=[suite.lastTestId])
  
  actual_products_count = spark.read.table(products_table).count()
  suite.testEquals(f"{suite_name}.p-count", f"Expected {meta_products_count} products, found {actual_products_count}", 
                   actual_products_count, meta_products_count, dependsOn=[suite.lastTestId])

  suite.test(f"{suite_name}.non-null-submitted_at", f"Non-null (properly parsed) submitted_at", dependsOn=[suite.lastTestId],
             testFunction = lambda: spark.read.table(orders_table).filter(col("submitted_at").isNull()).count() == 0)
  
  daLogger.logEvent(f"{suite_name}", f"{{\"registration_id\": {registration_id}, \"passed\": {suite.passed}, \"percentage\": {suite.percentage}, \"actPoints\": {suite.score}, \"maxPoints\": {suite.maxScore}}}")
  
  daLogger.logEvent(f"ex.02.final", f"{{\"registration_id\": {registration_id}, \"passed\": {TestResultsAggregator.passed}, \"percentage\": {TestResultsAggregator.percentage}, \"actPoints\": {TestResultsAggregator.score}, \"maxPoints\":   {TestResultsAggregator.maxScore}}}")
  
  suite.displayResults()