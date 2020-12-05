# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md # Exercise #0 - Project Overview
# MAGIC 
# MAGIC The capstone project aims to assess rudimentary skills as it relates to the Apache Spark and DataFrame APIs.
# MAGIC 
# MAGIC The approach taken here assumes that you are familiar with and have some experience with the following entities:
# MAGIC * **`SparkContext`**
# MAGIC * **`SparkSession`**
# MAGIC * **`DataFrame`**
# MAGIC * **`DataFrameReader`**
# MAGIC * **`DataFrameWriter`**
# MAGIC * The various functions found in the module **`pyspark.sql.functions`**
# MAGIC 
# MAGIC Throughout this project, you will be given specific instructions and it is our expectation that you will be able to complete these instructions drawing on your existing knowledge as well as other sources such as the Spark API Documentation.

# COMMAND ----------

# MAGIC %md ## The Project
# MAGIC 
# MAGIC The idea behind this project is to ingest data from a purchasing system and load it into a data lake for further analysis. 
# MAGIC 
# MAGIC Each exercise is broken up into smaller steps, or milestones.
# MAGIC 
# MAGIC After every milestone, we have provided a "reality check" to help ensure that you are progressing as expected.
# MAGIC 
# MAGIC Please note, because each exercise builds on the previous, it is essential to complete all exercises and ensure their "reality checks" pass, before moving on to the next exercise.
# MAGIC 
# MAGIC As the last exercise of this project, we will use this data we loaded to answer some simple business questions.

# COMMAND ----------

# MAGIC %md ## The Data
# MAGIC The raw data comes in three forms:
# MAGIC 
# MAGIC 1. Orders that were processed in 2017, 2018 and 2019.
# MAGIC   * For each year a separate batch (or backup) of that year's orders was produced
# MAGIC   * The format of all three files are similar, but were not produced exactly the same:
# MAGIC     * 2017 is in a fixed-width text file format
# MAGIC     * 2018 is tab-separated text file
# MAGIC     * 2019 is comma-separated text file
# MAGIC   * Each order consists for four main data points:
# MAGIC     0. The order - the highest level aggregate
# MAGIC     0. The line items - the individual products purchased in the order
# MAGIC     0. The sales reps - the person placing the order
# MAGIC     0. The customer - the person who purchased the items and where it was shipped.
# MAGIC   * All three batches are consistent in that there is one record per line item creating a significant amount of duplicated data across orders, reps and customers.
# MAGIC   * All entities are generally referenced by an ID, such as order_id, customer_id, etc.
# MAGIC   
# MAGIC 2. All products to be sold by this company (SKUs) are represented in a single XML file
# MAGIC 
# MAGIC 3. In 2020, the company switched systems and now lands a single JSON file in cloud storage for every order received.
# MAGIC   * These orders are simplified versions of the batched data fro 2017-2019 and includes only the order's details, the line items, and the correlating ids
# MAGIC   * The sales reps's data is no longer represented in conjunction with an order

# COMMAND ----------

# MAGIC %md ## The Exercises
# MAGIC 
# MAGIC * In **Exercise #1**, we will "install" a copy of the datasets into your Databricks workspace.
# MAGIC 
# MAGIC * In **Exercise #2**, we will ingest the batch data for 2017-2019, combine them into a single dataset for future processing.
# MAGIC 
# MAGIC * In **Exercise #3**, we will take the unified batch data from **Exercise #2**, clean it, and extract it into three new datasets: Orders, Line Items and Sales Reps. The customer data, for the sake of simplicity, will not be broken out and left with the orders.
# MAGIC 
# MAGIC * In **Exercise #4**, we will ingest the XML document containing all the projects, and combine it with the Line Items to create yet another dataset, Product Line Items.
# MAGIC 
# MAGIC * In **Exercise #5**, we will begin processing the stream of orders for 2020, appending that stream of data to the existing datasets as necessary.
# MAGIC 
# MAGIC * In **Exercise #6**, we will use all of our new datasets to answer a handful of business questions.