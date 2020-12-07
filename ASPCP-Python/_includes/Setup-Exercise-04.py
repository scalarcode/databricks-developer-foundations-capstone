# Databricks notebook source
# MAGIC %run ./Setup-Common

# COMMAND ----------

html = "<html><body><table>"
html += """<p style="font-size:16px">The following variables have been defined for you.<br/>Please refer to them in the following instructions."""
html += """<tr><th style="padding: 0 1em 0 0; text-align:left">Variable</th><th style="padding: 0 1em 0 0; text-align:left">Value</th><th style="padding: 0 1em 0 0; text-align:left">Description</th></tr>"""

html += html_row("username", username, """This is the email address that you signed into Databricks with. It is used here to create a "globally unique" working directory.""")
html += html_row("working_dir", working_dir, """This is directory in which all work should be conducted. This helps to ensure there are not conflicts with other users in the workspace.""")
html += html_row("user_db", user_db, """The name of the database you will use for this project.""")
html += html_row("products_table", products_table, """The name of the products table.""")
html += html_row("line_items_table", line_items_table, """The name of the line items table.""")
html += html_row("products_xml_path", products_xml_path, "The location of the product's XML file")

html += "</table></body></html>"

displayHTML(html)

# COMMAND ----------

def createExpectedProductSchema():
  from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType, IntegerType, TimestampType, DoubleType, DecimalType, BooleanType
  return StructType([StructField("product_id", StringType(), True),
                     StructField("color", StringType(), True),
                     StructField("model_name", StringType(), True),
                     StructField("model_number", StringType(), True),
                     StructField("base_price", DoubleType(), True),
                     StructField("color_adj", DoubleType(), True),
                     StructField("size_adj", DoubleType(), True),
                     StructField("price", DoubleType(), True),
                     StructField("size", StringType(), True),
                    ])
  
expectedProductSchema = createExpectedProductSchema()

def createExpectedProductLineItemSchema():
  from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType, IntegerType, TimestampType, DoubleType, DecimalType, BooleanType
  return StructType([StructField("product_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_quantity", IntegerType(), True),
    StructField("product_sold_price", DecimalType(10,2), True),
    StructField("color", StringType(), True),
    StructField("model_name", StringType(), True),
    StructField("model_number", StringType(), True),
    StructField("base_price", DecimalType(10,2), True),
    StructField("color_adj", DecimalType(10,1), True),
    StructField("size_adj", DecimalType(10,2), True),
    StructField("price", DecimalType(10,2), True),
    StructField("size", StringType(), True),
                    ])
  
expectedProductLineItemSchema = createExpectedProductLineItemSchema()

check_a_passed = False
check_b_passed = False
check_c_passed = False
check_final_passed = False

# COMMAND ----------

def reality_check_04_A():
  global check_a_passed
  
  suite_name = "ex.04.a"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.current-db", f"The current database is {user_db}",  
             testFunction = lambda: spark.catalog.currentDatabase() == user_db)

  daLogger.logEvent(f"{suite_name}", f"{{\"registration_id\": {registration_id}, \"passed\": {suite.passed}, \"percentage\": {suite.percentage}, \"actPoints\": {suite.score}, \"maxPoints\": {suite.maxScore}}}")
  
  check_a_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_04_B():
  global check_b_passed
  
  suite_name = "ex.04.b"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.spark-xml", f"Successfully installed the spark-xml library",
         testFunction = lambda: spark.read.format("xml").option("rootTag", "products").option("rowTag", "product").load(products_xml_path).count() == meta_products_count+1)
  
  check_b_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def reality_check_04_C():
  global check_c_passed
  from pyspark.sql.functions import min, max

  suite_name = "ex.04.c"
  suite = TestSuite()
  
  suite.test(f"{suite_name}.table-exists", f"The table {products_table} exists",  
             testFunction = lambda: len(list(filter(lambda t: t.name==products_table, spark.catalog.listTables(user_db)))) == 1)
  
  suite.test(f"{suite_name}.is-managed", f"The table {products_table} is a managed table", dependsOn=[suite.lastTestId()],
             testFunction = lambda: list(filter(lambda t: t.name==products_table, spark.catalog.listTables(user_db)))[0].tableType == "MANAGED")
  
  hive_path = f"dbfs:/user/hive/warehouse/{user_db}.db/{products_table}"
  suite.test(f"{suite_name}.is_delta", "Using the Delta file format", dependsOn=[suite.lastTestId()],
           testFunction = lambda: len(list(filter(lambda f: f.path.endswith("/_delta_log/"), dbutils.fs.ls( hive_path)))) == 1)

  suite.test(f"{suite_name}.schema", "Schema is valid", dependsOn=[suite.lastTestId()],
             testFunction = lambda: checkSchema(spark.read.table(products_table).schema, expectedProductSchema, False, False))

  actual = spark.read.table(products_table).count()
  suite.testEquals(f"{suite_name}.count", f"Expected {meta_products_count} records, found {actual}", 
                   actual, meta_products_count, dependsOn=[suite.lastTestId()])

  suite.test(f"{suite_name}.min-color_adj", f"Sample A of color_adj (valid values)", dependsOn=[suite.lastTestId()],
             testFunction = lambda: spark.read.table(products_table).select(min(col("color_adj"))).first()[0] == 1.0)

  suite.test(f"{suite_name}.max-color_adj", f"Sample B of color_adj (valid values)", dependsOn=[suite.lastTestId()],
             testFunction = lambda: spark.read.table(products_table).select(max(col("color_adj"))).first()[0] == 1.1)

  suite.test(f"{suite_name}.min-size_adj", f"Sample A of size_adj (valid values)", dependsOn=[suite.lastTestId()],
             testFunction = lambda: spark.read.table(products_table).select(min(col("size_adj"))).first()[0] == 0.9)

  suite.test(f"{suite_name}.max-size_adj", f"Sample B of size_adj (valid values)", dependsOn=[suite.lastTestId()],
             testFunction = lambda: spark.read.table(products_table).select(max(col("size_adj"))).first()[0] == 1.0)

  daLogger.logEvent(f"{suite_name}", f"{{\"registration_id\": {registration_id}, \"passed\": {suite.passed}, \"percentage\": {suite.percentage}, \"actPoints\": {suite.score}, \"maxPoints\": {suite.maxScore}}}")
  
  check_c_passed = suite.passed
  suite.displayResults()

# COMMAND ----------

def full_assessment_04():
  global check_final_passed
  from pyspark.sql.functions import col

  suite_name = "ex.04.all"
  suite = TestSuite()
  
  suite.testEquals(f"{suite_name}.a-passed", "Reality Check 04.A passed", check_a_passed, True)
  suite.testEquals(f"{suite_name}.b-passed", "Reality Check 04.B passed", check_b_passed, True, dependsOn=[suite.lastTestId()])
  suite.testEquals(f"{suite_name}.c-passed", "Reality Check 04.C passed", check_c_passed, True, dependsOn=[suite.lastTestId()])

  check_final_passed = suite.passed

  daLogger.logEvent(f"{suite_name}", f"{{\"registration_id\": {registration_id}, \"passed\": {suite.passed}, \"percentage\": {suite.percentage}, \"actPoints\": {suite.score}, \"maxPoints\": {suite.maxScore}}}")
  
  daLogger.logEvent(f"ex.02.final", f"{{\"registration_id\": {registration_id}, \"passed\": {TestResultsAggregator.passed}, \"percentage\": {TestResultsAggregator.percentage}, \"actPoints\": {TestResultsAggregator.score}, \"maxPoints\":   {TestResultsAggregator.maxScore}}}")
  
  suite.displayResults()