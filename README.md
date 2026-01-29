📊 Google Play Store ETL Pipeline (PySpark & MySQL)
📌 Project Overview

In this project, I act as a Data Engineer working for a company developing mobile software products.
The company aims to analyze the Google Play Store market in order to identify opportunities and decide on its next application to build.

To achieve this, we rely on raw datasets provided by Google, which are already extracted but not directly usable for analysis or decision-making.

The objective of this project is to design and implement a complete ETL (Extract, Transform, Load) pipeline, written entirely in Python with PySpark, that:

Downloads raw data automatically,

Cleans and transforms the datasets,

Loads the final, structured data into a MySQL SQL database.

This repository contains a single end-to-end PySpark script that performs the full pipeline when executed.

🏗️ Architecture & Technologies

Technologies used

Python

PySpark

Spark SQL

Docker

MySQL 8

JDBC (Spark ↔ MySQL Connector)

Pipeline structure

Data Source (S3)
        ↓
Extract (Python – urllib)
        ↓
Transform (PySpark)
        ↓
Load (JDBC → MySQL)

📂 Datasets

The datasets are hosted on an AWS S3 public bucket:

Applications metadata

gps_app.csv

URL:
https://assets-datascientest.s3.eu-west-1.amazonaws.com/gps_app.csv

User reviews & sentiment analysis

gps_user.csv

URL:
https://assets-datascientest.s3.eu-west-1.amazonaws.com/gps_user.csv

gps_app.csv – Main Columns

App – Application name

Category – Application category

Rating – App rating (0–5)

Reviews – Number of reviews

Size – App size

Installs – Number of installs

Type – Free / Paid

Price – App price

Content Rating – Age restriction

Genres – Sub-category

Last Updated – Last update date

Current Ver – Current version

Android Ver – Minimum Android version

gps_user.csv – Main Columns

App – Application name

Translated_Review – User review (English)

Sentiment – Sentiment label

Sentiment_Polarity – Sentiment polarity

Sentiment_Subjectivity – Sentiment subjectivity

🔄 ETL Pipeline Description
1️⃣ Extract

Automatic download of both CSV files using:

urllib.request.urlretrieve

The pipeline is fully automated and reproducible.

2️⃣ Transform
🔹 Global preprocessing

Standardization of column names:

lowercase

spaces replaced with underscores

🔹 Cleaning gps_app dataset

Missing value handling:

rating replaced by mean/median (robust to outliers)

type filled with the most logical value (Free)

Invalid type values cleaned

Remaining missing values filled using modal values

Data type corrections:

reviews → integer

installs → integer (regex-based cleaning)

price → double (currency handling)

last_updated → date (MMMM d, yyyy format)

🔹 Cleaning gps_user dataset

Analysis of missing values and row-wise consistency

Removal of rows with invalid or missing sentiment data

Validation and conversion of:

sentiment_polarity

sentiment_subjectivity

Text preprocessing on translated_review:

Removal of special characters

Whitespace normalization

Lowercasing

Review length analysis:

Distribution from 1 to 10 characters

Filtering reviews with length ≥ 3

Text analytics:

Extraction of the top 20 most frequent words in positive reviews using RDD MapReduce

3️⃣ Load

A MySQL 8 database is launched using Docker

Spark connects to MySQL via JDBC

Final DataFrames are stored as SQL tables:

gps_app

gps_user

🐳 MySQL Setup (Docker)
docker run --name my-mysql -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=my-secret-pw \
  -e MYSQL_USER=user \
  -e MYSQL_PASSWORD=password \
  -e MYSQL_DATABASE=database \
  -v mysql-data:/var/lib/mysql \
  -d mysql:8.3

🔌 Spark ↔ MySQL Connector
wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j-8.3.0.tar.gz
tar -xvf mysql-connector-j-8.3.0.tar.gz
mv mysql-connector-j-8.3.0/mysql-connector-j-8.3.0.jar .
rm -rf mysql-connector-j-8.3.0*


Launch PySpark:

pyspark --driver-class-path mysql-connector-j-8.3.0.jar \
        --jars mysql-connector-j-8.3.0.jar

🚀 Execution

The project is delivered as a single Python file.

To run the full ETL pipeline:

spark-submit etl_google_play_store.py


Prerequisites

MySQL container running

Spark available

JDBC connector present

Once executed, the script:

Downloads the datasets

Applies all transformations

Loads clean data into MySQL

✅ Final Output

Two clean, structured SQL tables:

gps_app

gps_user

A fully automated, production-ready ETL pipeline

Clear separation between extraction, transformation, and loading

🎯 Key Skills Demonstrated

End-to-end ETL design

PySpark data cleaning & transformations

Data quality handling

Text preprocessing & basic NLP

SQL database integration

Dockerized data infrastructure

Production-oriented scripting


