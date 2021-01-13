# Developer-Foundations-Capstone

## Summary 
This capstone project serves to validate an individual’s readiness for future classes such as the course Data Engineering with Databricks and Scalable Machine Learning on Databricks.

## Description
The Apache Spark Programming Capstone helps students put into practice skills learned in its prerequisite course, Apache Spark Programming with Databricks. This capstone also serves to validate participants’ readiness for the next courses in this series of partners’ classes, Data Engineering with Databricks and Machine Learning on Databricks.

In this capstone project, participants will engage in very simple ETL operations: to read data from multiple data sources; to produce new datasets by unioning and joining the source datasets; to employing various, prescribed transformations; to load the datasets back to disk in various forms. In addition to this, the participant will also leverage the Delta and Structured Streaming APIs to create a simple, streaming, pipeline as prescribed in the capstone project.

Participants are not expected to have an in-depth knowledge of the Spark and DataFrames APIs but should have some familiarity with the Spark APIs (SparkContext, SparkSession, DataFrame, DataFrameReaders, DataFrameWriters, Row and Column), architectural concepts, and should be able to navigate the Spark docs to lookup how to employ various transformations and actions. In addition to the core spark components, candidates should have a working knowledge of the Structured Streaming and Delta APIs.

## Learning objectives
* This capstone will evaluate the learner’s ability to:
* Apply transformations and actions as prescribed in the project
* Employ the DataFrameReaders to ingest JSON, CSV, XML, Parquet, and Delta datasets
* Employ the DataFrameWriters to load data into Parquet and Delta tables with prescribed features
* Develop a Structured Stream job to ingest data and merge it with a Delta table
* Update configuration settings to control and tune the application
* Extract data from a DataFrame to answer simple business questions

## Prerequisites
* Intermediate to advanced experience with Python
* Introductory knowledge of the Spark APIs (SparkContext, SparkSession, DataFrame, DataFrameReader, DataFrameWriter, Row, Column)
* Introductory knowledge of the Spark Architecture

## Getting Started

### Overview
There are two ways to get stareted with this project:
1. Importing this git repo into your Databricks workspace using the new **Databricks Projects** feature.
2. Downloading the DBC from the releases tab and importing it into your Databricks workspace.

Once imported, (either via DBC or Databricks Projects), open the notebook **Exercise 00** for next steps

### Import via Databricks Projects
Note: At the time of writing this, Databricks Projects is in private preview and is not yet available to everyone. If you do not yet have access to this feature, please use the alternative method which is to import the DBC file (see next section)
1. Copy the git URL for this repo (use the HTTPS form)<br/>
https://github.com/databricks-academy/Apache-Spark-Programming-Capstone.git
2. Sign into your Databricks workspace
3. In the left-hand navigation panel, select the **Projects** icon
4. In the projects folder, select the folder that corresponds to your username/email address
5. Select **Create Project**
6. Select **Clone from Git repo** (should be selected by default)
7. In the **Git repo URL** field, paste the URL you copied in step #1 above
8. Select **Create** - after a few seconds, the project should be fully imported
9. Open the notebook, **Exercise 01 - Overview and Install**.

### Import DBC file
1. In GitHub, select the **Releases** tab<br/>
https://github.com/databricks-academy/Apache-Spark-Programming-Capstone/releases
2. Select the most recent release by clicking on the release title
3. Download the DBC file, **ASPCP-Python.dbc** - remember where you save this file to, you will need it in a few steps
4. Sign into your Databricks workspace
5. In the left-hand navigation panel, select the **Home** icon
6. In the home folder, select the folder that corresponds to your username/email address
7. To the right of your username/email, click the **down cheveron** (**&#8964;**)
8. Select the option to **Import**
9. Select **browse** and select the DBC file that you saved to your machine in step #3 above.
10. Select **Import** - after a few seconds, the project should be fully imported
9. Open the notebook, **Exercise 01 - Overview and Install**.

### Completing the Capstone Project
1. Start with notebook **Exercise 01 - Overview and Install** which provides an overview and context for the project.
2. From there, work your way through each notebook (1-6)
