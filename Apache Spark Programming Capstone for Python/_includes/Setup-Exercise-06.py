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

html += html_row("question_1_results_table", question_1_results_table, """The name of the temporary view for the results to question #1.""")
html += html_row("question_2_results_table", question_1_results_table, """The name of the temporary view for the results to question #2.""")
html += html_row("question_3_results_table", question_1_results_table, """The name of the temporary view for the results to question #3.""")

html += "</table></body></html>"

displayHTML(html)

# COMMAND ----------

check_a_passed = True
check_b_passed = True
check_c_passed = True
check_d_passed = True

# COMMAND ----------

def reality_check_06_A():
  from pyspark.sql.functions import explode 
  global check_a_passed
  
  suite_name = "ex.06.a"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.current-db", f"The current database is {user_db}",  
             testFunction = lambda: spark.catalog.currentDatabase() == user_db)

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
  
  actual_sales_reps_count = spark.read.table(sales_reps_table).count()
  suite.testEquals(f"{suite_name}.sr-count", f"Expected {meta_sales_reps_count} sales reps, found {actual_sales_reps_count}", 
                   actual_sales_reps_count, meta_sales_reps_count, dependsOn=[suite.lastTestId])

  daLogger.logEvent(f"{suite_name}", f"{{\"registration_id\": {registration_id}, \"passed\": {suite.passed}, \"percentage\": {suite.percentage}, \"actPoints\": {suite.score}, \"maxPoints\": {suite.maxScore}}}")
  
  check_a_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_06_B():
  global check_b_passed
  
  suite_name = "ex.06.b"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.table-exists", f"""The table "{question_1_results_table}" exists""",  
             testFunction = lambda: len(list(filter(lambda t: t.name==question_1_results_table, spark.catalog.listTables(user_db)))) == 1)
  
  suite.test(f"{suite_name}.is-temp-view", f"""The table "{question_1_results_table}" is a temp view""", dependsOn=[suite.lastTestId],
             testFunction = lambda: list(filter(lambda t: t.name==question_1_results_table, spark.catalog.listTables(user_db)))[0].tableType == "TEMPORARY")

  exp_q1 = spark.read.table(orders_table).groupBy("shipping_address_state").count().orderBy(col("count").desc()).collect()
  act_q1 = spark.read.table(question_1_results_table).collect()

  suite.testEquals(f"{suite_name}.q1-state", f"""Schema contains the column "shipping_address_state".""",
          "shipping_address_state" in spark.read.table(question_1_results_table).columns, True, dependsOn=[suite.lastTestId])
  suite.testEquals(f"{suite_name}.q1-count", f"""Schema contains the column "count".""",
          "count" in spark.read.table(question_1_results_table).columns, True, dependsOn=[suite.lastTestId])

  suite.testEquals(f"{suite_name}.q1-first-state", f"""Expected the first state to be {exp_q1[0]["shipping_address_state"]}, found {act_q1[0]["shipping_address_state"]}""",
            exp_q1[0]["shipping_address_state"], act_q1[0]["shipping_address_state"], dependsOn=[suite.lastTestId])
  suite.testEquals(f"{suite_name}.q1-first-count", f"""Expected the first count to be {exp_q1[0]["count"]}, found {act_q1[0]["count"]}""",
            exp_q1[0]["count"], act_q1[0]["count"], dependsOn=[suite.lastTestId])

  suite.testEquals(f"{suite_name}.q1-last-state", f"""Expected the last state to be {exp_q1[-1]["shipping_address_state"]}, found {act_q1[-1]["shipping_address_state"]}""",
            exp_q1[-1]["shipping_address_state"], act_q1[-1]["shipping_address_state"], dependsOn=[suite.lastTestId])
  suite.testEquals(f"{suite_name}.q1-last-count", f"""Expected the last count to be {exp_q1[-1]["count"]}, found {act_q1[-1]["count"]}""",
            exp_q1[-1]["count"], act_q1[-1]["count"], dependsOn=[suite.lastTestId])
  
  daLogger.logEvent(f"{suite_name}", f"{{\"registration_id\": {registration_id}, \"passed\": {suite.passed}, \"percentage\": {suite.percentage}, \"actPoints\": {suite.score}, \"maxPoints\": {suite.maxScore}}}")
  
  check_b_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_06_C(ex_avg, ex_min, ex_max):
  from pyspark.sql.functions import avg, min, max
  global check_c_passed
  
  suite_name = "ex.06.c"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.table-exists", f"""The table "{question_2_results_table}" exists""",  
             testFunction = lambda: len(list(filter(lambda t: t.name==question_2_results_table, spark.catalog.listTables(user_db)))) == 1)
  
  suite.test(f"{suite_name}.is-temp-view", f"""The table "{question_2_results_table}" is a temp view""", dependsOn=[suite.lastTestId],
             testFunction = lambda: list(filter(lambda t: t.name==question_2_results_table, spark.catalog.listTables(user_db)))[0].tableType == "TEMPORARY")

  suite.testEquals(f"{suite_name}.q2-avg", f"""Schema contains the column "avg(product_sold_price)".""",
          "avg(product_sold_price)" in spark.read.table(question_2_results_table).columns, True, dependsOn=[suite.lastTestId])
  suite.testEquals(f"{suite_name}.q2-min", f"""Schema contains the column "min(product_sold_price)".""",
          "min(product_sold_price)" in spark.read.table(question_2_results_table).columns, True, dependsOn=[suite.lastTestId])
  suite.testEquals(f"{suite_name}.q2-max", f"""Schema contains the column "max(product_sold_price)".""",
          "max(product_sold_price)" in spark.read.table(question_2_results_table).columns, True, dependsOn=[suite.lastTestId])

  
  
  act_results = spark.read.table(question_2_results_table).first()
  act_avg = act_results["avg(product_sold_price)"]
  act_min = act_results["min(product_sold_price)"]
  act_max = act_results["max(product_sold_price)"]
  
  exp_results = spark.read.table(orders_table).join(spark.read.table(sales_reps_table), "sales_rep_id").join(spark.read.table(line_items_table), "order_id").join(spark.read.table(products_table), "product_id").filter(col("shipping_address_state") == "NC").filter(col("_error_ssn_format") == True).filter(col("color") == "green").select(avg("product_sold_price"), min("product_sold_price"), max("product_sold_price")).first()
  exp_avg = exp_results["avg(product_sold_price)"]
  exp_min = exp_results["min(product_sold_price)"]
  exp_max = exp_results["max(product_sold_price)"]

  suite.testEquals(f"{suite_name}.q2-tv-avg", f"""Expected the temp view's average to be {exp_avg}, found {act_avg}""", exp_avg, act_avg, dependsOn=[suite.lastTestId])
  suite.testEquals(f"{suite_name}.q2-tv-min", f"""Expected the temp view's minimum to be {exp_min}, found {act_min}""", exp_min, act_min, dependsOn=[suite.lastTestId])
  suite.testEquals(f"{suite_name}.q2-tv-max", f"""Expected the temp view's maximum to be {exp_max}, found {act_max}""", exp_max, act_max, dependsOn=[suite.lastTestId])

  suite.testEquals(f"{suite_name}.q2-ex-avg", f"""Expected the extracted average to be {exp_avg}, found {ex_avg}""", exp_avg, ex_avg, dependsOn=[suite.lastTestId])
  suite.testEquals(f"{suite_name}.q2-ex-min", f"""Expected the extracted minimum to be {exp_min}, found {ex_min}""", exp_min, ex_min, dependsOn=[suite.lastTestId])
  suite.testEquals(f"{suite_name}.q2-ex-max", f"""Expected the extracted maximum to be {exp_max}, found {ex_max}""", exp_max, ex_max, dependsOn=[suite.lastTestId])
  
  daLogger.logEvent(f"{suite_name}", f"{{\"registration_id\": {registration_id}, \"passed\": {suite.passed}, \"percentage\": {suite.percentage}, \"actPoints\": {suite.score}, \"maxPoints\": {suite.maxScore}}}")
  
  check_c_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_06_D():
  global check_c_passed
  
  suite_name = "ex.06.d"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.table-exists", f"""The table "{question_3_results_table}" exists""",  
             testFunction = lambda: len(list(filter(lambda t: t.name==question_3_results_table, spark.catalog.listTables(user_db)))) == 1)
  
  suite.test(f"{suite_name}.is-temp-view", f"""The table "{question_3_results_table}" is a temp view""", dependsOn=[suite.lastTestId],
             testFunction = lambda: list(filter(lambda t: t.name==question_3_results_table, spark.catalog.listTables(user_db)))[0].tableType == "TEMPORARY")

  suite.testEquals(f"{suite_name}.q2-fn", f"""Schema contains the column "sales_rep_first_name".""",
          "sales_rep_first_name" in spark.read.table(question_3_results_table).columns, True, dependsOn=[suite.lastTestId])
  suite.testEquals(f"{suite_name}.q2-ln", f"""Schema contains the column "sales_rep_last_name".""",
          "sales_rep_last_name" in spark.read.table(question_3_results_table).columns, True, dependsOn=[suite.lastTestId])

  suite.test(f"{suite_name}.count", f"""Expected 1 record, found {spark.read.table(question_3_results_table).count()}""", dependsOn=[suite.lastTestId],
             testFunction = lambda: spark.read.table(question_3_results_table).count() == 1)

  act_results = spark.read.table(question_3_results_table).first()
  act_first = act_results["sales_rep_first_name"]
  act_last = act_results["sales_rep_last_name"]
  
  exp_results = (spark.read.table(orders_table).join(
                 spark.read.table(sales_reps_table), "sales_rep_id").join(
                 spark.read.table(line_itmes_table), "order_id").join(
                 spark.read.table(products_table), "product_id").withColumn("per_product_profit", col("product_sold_price") - col("price")).withColumn("total_profit", col("per_product_profit") * col("product_quantity")).groupBy("sales_rep_id", "sales_rep_first_name", "sales_rep_last_name").sum("total_profit").orderBy(col("sum(total_profit)").desc()).first())
  exp_first = exp_results["sales_rep_first_name"]
  exp_last = exp_results["sales_rep_last_name"]

  suite.testEquals(f"{suite_name}.q3-fn", f"""Expected the first name to be {exp_first}, found {act_first}""", exp_first, act_first, dependsOn=[suite.lastTestId])
  suite.testEquals(f"{suite_name}.q3-ln", f"""Expected the last name to be {exp_last}, found {act_last}""", exp_last, act_last, dependsOn=[suite.lastTestId])
  
  daLogger.logEvent(f"{suite_name}", f"{{\"registration_id\": {registration_id}, \"passed\": {suite.passed}, \"percentage\": {suite.percentage}, \"actPoints\": {suite.score}, \"maxPoints\": {suite.maxScore}}}")
  
  check_d_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def full_assessment_06():
  from pyspark.sql.functions import explode
  
  suite_name = "ex.06.all"
  suite = TestSuite()
  
  suite.testEquals(f"{suite_name}.a-passed", "Reality Check 06.A passed", check_a_passed, True)
  suite.testEquals(f"{suite_name}.b-passed", "Reality Check 06.B passed", check_b_passed, True, dependsOn=[suite.lastTestId])
  suite.testEquals(f"{suite_name}.c-passed", "Reality Check 06.C passed", check_c_passed, True, dependsOn=[suite.lastTestId])
  suite.testEquals(f"{suite_name}.d-passed", "Reality Check 06.D passed", check_c_passed, True, dependsOn=[suite.lastTestId])

  daLogger.logEvent(f"{suite_name}", f"{{\"registration_id\": {registration_id}, \"passed\": {suite.passed}, \"percentage\": {suite.percentage}, \"actPoints\": {suite.score}, \"maxPoints\": {suite.maxScore}}}")
  
  daLogger.logEvent(f"ex.02.final", f"{{\"registration_id\": {registration_id}, \"passed\": {TestResultsAggregator.passed}, \"percentage\": {TestResultsAggregator.percentage}, \"actPoints\": {TestResultsAggregator.score}, \"maxPoints\":   {TestResultsAggregator.maxScore}}}")
  
  suite.displayResults()