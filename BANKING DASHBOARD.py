# Databricks notebook source
# MAGIC %md
# MAGIC # STEP 1: INITIAL SETUP AND IMPORTS

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("HomeCreditDashboard").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC # BRONZE LAYER (Raw Data Ingestion)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # STEP 2: DATA INGESTION - READING CSV FILES
# MAGIC

# COMMAND ----------

# Read application_train.csv
application_train = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/workspace/home_credit_data/credit_volume/application_train.csv")

print(f"Application Train Data: {application_train.count()} rows, {len(application_train.columns)} columns")

# Read application_test.csv
application_test = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/workspace/home_credit_data/credit_volume/application_test.csv")

print(f"Application Test Data: {application_test.count()} rows, {len(application_test.columns)} columns")

# Read bureau.csv
bureau = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/workspace/home_credit_data/credit_volume/bureau.csv")

print(f"Bureau Data: {bureau.count()} rows, {len(bureau.columns)} columns")

# Read bureau_balance.csv
bureau_balance = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/workspace/home_credit_data/credit_volume/bureau_balance.csv")

print(f"Bureau Balance Data: {bureau_balance.count()} rows, {len(bureau_balance.columns)} columns")

# Read previous_application.csv
previous_application = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/workspace/home_credit_data/credit_volume/previous_application.csv")

print(f"Previous Application Data: {previous_application.count()} rows, {len(previous_application.columns)} columns")

# Read POS_CASH_balance.csv
pos_cash_balance = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/workspace/home_credit_data/credit_volume/POS_CASH_balance.csv")

print(f"POS Cash Balance Data: {pos_cash_balance.count()} rows, {len(pos_cash_balance.columns)} columns")

# Read installments_payments.csv
installments_payments = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/workspace/home_credit_data/credit_volume/installments_payments.csv")

print(f"Installments Payments Data: {installments_payments.count()} rows, {len(installments_payments.columns)} columns")

# Read credit_card_balance.csv
credit_card_balance = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/workspace/home_credit_data/credit_volume/credit_card_balance.csv")

print(f"Credit Card Balance Data: {credit_card_balance.count()} rows, {len(credit_card_balance.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC # STEP 3: DATA EXPLORATION AND BASIC INFO

# COMMAND ----------

datasets = {
    'application_train': application_train,
    'application_test': application_test,
    'bureau': bureau,
    'bureau_balance': bureau_balance,
    'previous_application': previous_application,
    'pos_cash_balance': pos_cash_balance,
    'installments_payments': installments_payments,
    'credit_card_balance': credit_card_balance
}

for name, df in datasets.items():
    print(f"\n{name.upper()} DATASET:")
    print(f"Rows: {df.count()}")
    print(f"Columns: {len(df.columns)}")
    df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # SILVER LAYER (Cleaned & Standardized Data)

# COMMAND ----------

# MAGIC %md
# MAGIC # STEP 4: DATA CLEANING - APPLICATION TRAIN

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove rows with missing target values

# COMMAND ----------

application_train_clean = application_train.filter(col("TARGET").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove extreme outliers in income (keeping realistic values)

# COMMAND ----------

application_train_clean = application_train_clean.filter(
    (col("AMT_INCOME_TOTAL") >= 10000) & 
    (col("AMT_INCOME_TOTAL") <= 10000000)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove extreme outliers in credit amount

# COMMAND ----------

application_train_clean = application_train_clean.filter(
    (col("AMT_CREDIT") >= 10000) & 
    (col("AMT_CREDIT") <= 50000000)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix negative values in DAYS columns (they represent days before application)

# COMMAND ----------

application_train_clean = application_train_clean.withColumn(
    "DAYS_BIRTH", abs(col("DAYS_BIRTH"))
).withColumn(
    "DAYS_EMPLOYED", abs(col("DAYS_EMPLOYED"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create age in years

# COMMAND ----------

application_train_clean = application_train_clean.withColumn(
    "AGE_YEARS", floor(col("DAYS_BIRTH") / 365.25)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Create credit to income ratio

# COMMAND ----------

application_train_clean = application_train_clean.withColumn(
    "CREDIT_INCOME_RATIO", 
    when(col("AMT_INCOME_TOTAL") > 0, col("AMT_CREDIT") / col("AMT_INCOME_TOTAL")).otherwise(0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create employment years

# COMMAND ----------

application_train_clean = application_train_clean.withColumn(
    "EMPLOYMENT_YEARS", 
    when(col("DAYS_EMPLOYED") < 365243, floor(col("DAYS_EMPLOYED") / 365.25)).otherwise(0)
)

print(f"Application Train - After cleaning: {application_train_clean.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC # STEP 5: DATA CLEANING - BUREAU DATA

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean bureau data

# COMMAND ----------

bureau_clean = bureau.filter(col("SK_ID_CURR").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove extreme outliers in credit amounts

# COMMAND ----------

bureau_clean = bureau_clean.filter(
    (col("AMT_CREDIT_SUM").isNull()) | 
    ((col("AMT_CREDIT_SUM") >= 0) & (col("AMT_CREDIT_SUM") <= 100000000))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix negative DAYS_CREDIT (days before current application)

# COMMAND ----------

bureau_clean = bureau_clean.withColumn(
    "DAYS_CREDIT", abs(col("DAYS_CREDIT"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate credit utilization ratio

# COMMAND ----------

bureau_clean = bureau_clean.withColumn(
    "CREDIT_UTILIZATION_RATIO",
    when((col("AMT_CREDIT_SUM").isNotNull()) & (col("AMT_CREDIT_SUM") > 0) & (col("AMT_CREDIT_SUM_DEBT").isNotNull()),
         col("AMT_CREDIT_SUM_DEBT") / col("AMT_CREDIT_SUM")
    ).otherwise(0)
)

print(f"Bureau - After cleaning: {bureau_clean.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC # STEP 6: DATA CLEANING - PREVIOUS APPLICATIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean previous applications

# COMMAND ----------

previous_application_clean = previous_application.filter(col("SK_ID_CURR").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove extreme outliers in application amounts

# COMMAND ----------

previous_application_clean = previous_application_clean.filter(
    (col("AMT_APPLICATION").isNull()) | 
    ((col("AMT_APPLICATION") >= 0) & (col("AMT_APPLICATION") <= 50000000))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix negative days values

# COMMAND ----------

previous_application_clean = previous_application_clean.withColumn(
    "DAYS_DECISION", abs(col("DAYS_DECISION"))
)

print(f"Previous Application - After cleaning: {previous_application_clean.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC # STEP 7: DATA CLEANING - INSTALLMENTS PAYMENTS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean installments payments

# COMMAND ----------

installments_payments_clean = installments_payments.filter(col("SK_ID_CURR").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove rows with missing payment information

# COMMAND ----------

installments_payments_clean = installments_payments_clean.filter(
    col("AMT_PAYMENT").isNotNull() & 
    col("AMT_INSTALMENT").isNotNull()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate payment difference and days late

# COMMAND ----------

installments_payments_clean = installments_payments_clean.withColumn(
    "PAYMENT_DIFFERENCE", col("AMT_PAYMENT") - col("AMT_INSTALMENT")
).withColumn(
    "DAYS_LATE", col("DAYS_ENTRY_PAYMENT") - col("DAYS_INSTALMENT")
)

print(f"Installments Payments - After cleaning: {installments_payments_clean.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC # STEP 8: DATA CLEANING - BUREAU BALANCE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean bureau balance data

# COMMAND ----------

bureau_balance_clean = bureau_balance.filter(col("SK_ID_BUREAU").isNotNull())

print(f"Bureau Balance - After cleaning: {bureau_balance_clean.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC # STEP 9: CREATE AGGREGATED FEATURES

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate bureau features per customer

# COMMAND ----------

bureau_features = bureau_clean.groupBy("SK_ID_CURR").agg(
    count("SK_ID_BUREAU").alias("BUREAU_CREDIT_COUNT"),
    sum(when(col("CREDIT_ACTIVE") == "Active", 1).otherwise(0)).alias("ACTIVE_CREDIT_COUNT"),
    sum(when(col("CREDIT_ACTIVE") == "Closed", 1).otherwise(0)).alias("CLOSED_CREDIT_COUNT"),
    avg("AMT_CREDIT_SUM").alias("AVG_CREDIT_AMOUNT"),
    sum("AMT_CREDIT_SUM_DEBT").alias("TOTAL_DEBT"),
    avg("CREDIT_UTILIZATION_RATIO").alias("AVG_CREDIT_UTILIZATION"),
    max("DAYS_CREDIT").alias("DAYS_LAST_CREDIT")
)

print(f"Bureau features created for {bureau_features.count()} customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate previous application features

# COMMAND ----------

prev_app_features = previous_application_clean.groupBy("SK_ID_CURR").agg(
    count("SK_ID_PREV").alias("TOTAL_PREV_APPLICATIONS"),
    sum(when(col("NAME_CONTRACT_STATUS") == "Approved", 1).otherwise(0)).alias("APPROVED_APPLICATIONS"),
    sum(when(col("NAME_CONTRACT_STATUS") == "Refused", 1).otherwise(0)).alias("REFUSED_APPLICATIONS"),
    sum(when(col("NAME_CONTRACT_STATUS") == "Canceled", 1).otherwise(0)).alias("CANCELED_APPLICATIONS"),
    avg("AMT_APPLICATION").alias("AVG_PREV_APPLICATION_AMOUNT"),
    max("DAYS_DECISION").alias("DAYS_LAST_DECISION")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate approval rate

# COMMAND ----------

prev_app_features = prev_app_features.withColumn(
    "APPROVAL_RATE",
    when(col("TOTAL_PREV_APPLICATIONS") > 0, 
         col("APPROVED_APPLICATIONS") / col("TOTAL_PREV_APPLICATIONS") * 100
    ).otherwise(0)
)

print(f"Previous application features created for {prev_app_features.count()} customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate installment payment behavior

# COMMAND ----------

payment_features = installments_payments_clean.groupBy("SK_ID_CURR").agg(
    count("SK_ID_PREV").alias("TOTAL_PAYMENTS"),
    avg("PAYMENT_DIFFERENCE").alias("AVG_PAYMENT_DIFFERENCE"),
    avg("DAYS_LATE").alias("AVG_DAYS_LATE"),
    sum(when(col("DAYS_LATE") > 0, 1).otherwise(0)).alias("LATE_PAYMENTS_COUNT"),
    max("DAYS_LATE").alias("MAX_DAYS_LATE")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate late payment rate

# COMMAND ----------

payment_features = payment_features.withColumn(
    "LATE_PAYMENT_RATE",
    when(col("TOTAL_PAYMENTS") > 0,
         col("LATE_PAYMENTS_COUNT") / col("TOTAL_PAYMENTS") * 100
    ).otherwise(0)
)

print(f"Payment behavior features created for {payment_features.count()} customers")

# COMMAND ----------

# MAGIC %md
# MAGIC #  GOLD LAYER (Business-Ready Analytics Data)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # STEP 10: CREATE MASTER DATASET

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join all features to main application data

# COMMAND ----------

master_dataset = application_train_clean.join(
    bureau_features, "SK_ID_CURR", "left"
).join(
    prev_app_features, "SK_ID_CURR", "left"
).join(
    payment_features, "SK_ID_CURR", "left"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fill null values with appropriate defaults

# COMMAND ----------

numeric_columns = [
    "BUREAU_CREDIT_COUNT", "ACTIVE_CREDIT_COUNT", "CLOSED_CREDIT_COUNT",
    "AVG_CREDIT_AMOUNT", "TOTAL_DEBT", "AVG_CREDIT_UTILIZATION",
    "TOTAL_PREV_APPLICATIONS", "APPROVED_APPLICATIONS", "REFUSED_APPLICATIONS",
    "CANCELED_APPLICATIONS", "APPROVAL_RATE", "TOTAL_PAYMENTS",
    "AVG_PAYMENT_DIFFERENCE", "AVG_DAYS_LATE", "LATE_PAYMENTS_COUNT",
    "LATE_PAYMENT_RATE"
]

# COMMAND ----------

for column in numeric_columns:
    master_dataset = master_dataset.fillna({column: 0})

print(f"Master dataset created: {master_dataset.count()} rows, {len(master_dataset.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC # STEP 11: CREATE ADDITIONAL DERIVED FEATURES
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create categorical features for analysis

# COMMAND ----------

master_dataset = master_dataset.withColumn(
    "AGE_GROUP",
    when(col("AGE_YEARS") <= 25, "Young (18-25)")
    .when(col("AGE_YEARS") <= 35, "Young Professional (26-35)")
    .when(col("AGE_YEARS") <= 45, "Established Adult (36-45)")
    .when(col("AGE_YEARS") <= 55, "Middle-aged (46-55)")
    .when(col("AGE_YEARS") <= 65, "Pre-retirement (56-65)")
    .otherwise("Senior (65+)")
)

master_dataset = master_dataset.withColumn(
    "INCOME_LEVEL",
    when(col("AMT_INCOME_TOTAL") <= 100000, "Low Income (≤100K)")
    .when(col("AMT_INCOME_TOTAL") <= 200000, "Lower Middle (100K-200K)")
    .when(col("AMT_INCOME_TOTAL") <= 300000, "Middle Income (200K-300K)")
    .when(col("AMT_INCOME_TOTAL") <= 500000, "Upper Middle (300K-500K)")
    .when(col("AMT_INCOME_TOTAL") <= 1000000, "High Income (500K-1M)")
    .otherwise("Very High Income (1M+)")
)

master_dataset = master_dataset.withColumn(
    "CREDIT_SIZE_CATEGORY",
    when(col("AMT_CREDIT") <= 200000, "Small Loans (≤200K)")
    .when(col("AMT_CREDIT") <= 500000, "Medium Loans (200K-500K)")
    .when(col("AMT_CREDIT") <= 1000000, "Large Loans (500K-1M)")
    .when(col("AMT_CREDIT") <= 2000000, "Very Large Loans (1M-2M)")
    .otherwise("Jumbo Loans (2M+)")
)

master_dataset = master_dataset.withColumn(
    "CREDIT_RISK_LEVEL",
    when(col("CREDIT_INCOME_RATIO") <= 1, "Conservative (≤1x Income)")
    .when(col("CREDIT_INCOME_RATIO") <= 2, "Moderate (1-2x Income)")
    .when(col("CREDIT_INCOME_RATIO") <= 3, "Aggressive (2-3x Income)")
    .otherwise("Very Aggressive (3x+ Income)")
)

master_dataset = master_dataset.withColumn(
    "CREDIT_HISTORY_LEVEL",
    when(col("BUREAU_CREDIT_COUNT") == 0, "No Credit History")
    .when(col("BUREAU_CREDIT_COUNT") <= 2, "Limited History (1-2)")
    .when(col("BUREAU_CREDIT_COUNT") <= 5, "Moderate History (3-5)")
    .when(col("BUREAU_CREDIT_COUNT") <= 10, "Extensive History (6-10)")
    .otherwise("Very Extensive (10+)")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # STEP 12: SAVE CLEANED DATASETS AS DELTA TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save master dataset

# COMMAND ----------

master_dataset.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("home_credit_data.master_dataset")

print("Master dataset saved as Delta table: home_credit.master_dataset")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save individual cleaned datasets

# COMMAND ----------

application_train_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("home_credit_data.application_train")

bureau_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("home_credit_data.bureau")

bureau_balance_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("home_credit_data.bureau_balance")

previous_application_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("home_credit_data.previous_application")

installments_payments_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("home_credit_data.installments_payments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View the cleaned datasets

# COMMAND ----------

application_train.display()

# COMMAND ----------

bureau.display()

# COMMAND ----------

bureau_balance.display()

# COMMAND ----------

previous_application.display()

# COMMAND ----------

installments_payments.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # STEP 13: CREATE VIEWS FOR DASHBOARD QUERIES
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register temporary views for SQL queries

# COMMAND ----------

master_dataset.createOrReplaceTempView("master_risk_data")
application_train_clean.createOrReplaceTempView("application_train")
bureau_clean.createOrReplaceTempView("bureau")
bureau_balance_clean.createOrReplaceTempView("bureau_balance")
previous_application_clean.createOrReplaceTempView("previous_application")
installments_payments_clean.createOrReplaceTempView("installments_payments")

print("Views created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC # STEP 14: DATA QUALITY SUMMARY

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show data completeness

# COMMAND ----------

print("\nDATA SUMMARY")

print(f"Master Dataset: {master_dataset.count():,} customers ready for analysis")
print(f"Default Rate: {master_dataset.agg(avg('TARGET')).collect()[0][0]:.2%}")
print(f"Average Age: {master_dataset.agg(avg('AGE_YEARS')).collect()[0][0]:.1f} years")
print(f"Average Income: ${master_dataset.agg(avg('AMT_INCOME_TOTAL')).collect()[0][0]:,.0f}")
print(f"Average Credit: ${master_dataset.agg(avg('AMT_CREDIT')).collect()[0][0]:,.0f}")


print("\nKEY FEATURES COMPLETENESS")
total_rows = master_dataset.count()

key_features = [
    'TARGET', 'CODE_GENDER', 'AGE_YEARS', 'AMT_INCOME_TOTAL', 'AMT_CREDIT',
    'NAME_EDUCATION_TYPE', 'NAME_FAMILY_STATUS', 'BUREAU_CREDIT_COUNT',
    'TOTAL_PREV_APPLICATIONS', 'CREDIT_INCOME_RATIO'
]

for feature in key_features:
    non_null_count = master_dataset.filter(col(feature).isNotNull()).count()
    completeness = (non_null_count / total_rows) * 100
    print(f"{feature}: {completeness:.1f}% complete ({non_null_count:,}/{total_rows:,})")

print("\nDATA PIPELINE COMPLETED SUCCESSFULLY!")

# COMMAND ----------

# MAGIC %md
# MAGIC # STEP 15: SAMPLE QUERIES FOR TESTING

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test query 1: Customer demographics

# COMMAND ----------

demo_result = spark.sql("""
SELECT 
    AGE_GROUP,
    COUNT(*) as CUSTOMER_COUNT,
    ROUND(AVG(TARGET) * 100, 2) as DEFAULT_RATE
FROM master_risk_data 
GROUP BY AGE_GROUP
ORDER BY DEFAULT_RATE DESC
""")

print("Customer Demographics by Age Group:")
demo_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test query 2: Risk by income level

# COMMAND ----------

risk_result = spark.sql("""
SELECT 
    INCOME_LEVEL,
    COUNT(*) as CUSTOMER_COUNT,
    ROUND(AVG(TARGET) * 100, 2) as DEFAULT_RATE,
    ROUND(AVG(AMT_CREDIT), 0) as AVG_LOAN_SIZE
FROM master_risk_data 
GROUP BY INCOME_LEVEL
ORDER BY DEFAULT_RATE DESC
""")

print("Risk Analysis by Income Level:")
display(risk_result)

# COMMAND ----------

# MAGIC %md
# MAGIC # CHAPTER 1: WHO ARE OUR CUSTOMERS?

# COMMAND ----------

# MAGIC %md
# MAGIC ##1.1 Customer Demographics Overview 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualization 1: Customer Age Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN FLOOR(ABS(DAYS_BIRTH)/365.25) BETWEEN 18 AND 25 THEN '18-25 (Young Adults)'
# MAGIC         WHEN FLOOR(ABS(DAYS_BIRTH)/365.25) BETWEEN 26 AND 35 THEN '26-35 (Young Professionals)'
# MAGIC         WHEN FLOOR(ABS(DAYS_BIRTH)/365.25) BETWEEN 36 AND 45 THEN '36-45 (Established Adults)'
# MAGIC         WHEN FLOOR(ABS(DAYS_BIRTH)/365.25) BETWEEN 46 AND 55 THEN '46-55 (Middle-aged)'
# MAGIC         WHEN FLOOR(ABS(DAYS_BIRTH)/365.25) BETWEEN 56 AND 65 THEN '56-65 (Pre-retirement)'
# MAGIC         ELSE '65+ (Senior Citizens)'
# MAGIC     END as AGE_GROUP,
# MAGIC     COUNT(*) as CUSTOMER_COUNT,
# MAGIC     ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM application_train), 2) as PERCENTAGE
# MAGIC FROM application_train 
# MAGIC WHERE DAYS_BIRTH IS NOT NULL
# MAGIC GROUP BY 
# MAGIC     CASE 
# MAGIC         WHEN FLOOR(ABS(DAYS_BIRTH)/365.25) BETWEEN 18 AND 25 THEN '18-25 (Young Adults)'
# MAGIC         WHEN FLOOR(ABS(DAYS_BIRTH)/365.25) BETWEEN 26 AND 35 THEN '26-35 (Young Professionals)'
# MAGIC         WHEN FLOOR(ABS(DAYS_BIRTH)/365.25) BETWEEN 36 AND 45 THEN '36-45 (Established Adults)'
# MAGIC         WHEN FLOOR(ABS(DAYS_BIRTH)/365.25) BETWEEN 46 AND 55 THEN '46-55 (Middle-aged)'
# MAGIC         WHEN FLOOR(ABS(DAYS_BIRTH)/365.25) BETWEEN 56 AND 65 THEN '56-65 (Pre-retirement)'
# MAGIC         ELSE '65+ (Senior Citizens)'
# MAGIC     END
# MAGIC ORDER BY MIN(FLOOR(ABS(DAYS_BIRTH)/365.25))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Visualization 2: Gender Composition 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gender Distribution
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN CODE_GENDER = 'M' THEN 'Male'
# MAGIC         WHEN CODE_GENDER = 'F' THEN 'Female'
# MAGIC         ELSE 'Not Specified'
# MAGIC     END as GENDER,
# MAGIC     COUNT(*) as CUSTOMER_COUNT,
# MAGIC     ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM application_train WHERE CODE_GENDER IN ('M', 'F')), 2) as PERCENTAGE
# MAGIC FROM application_train 
# MAGIC WHERE CODE_GENDER IN ('M', 'F')
# MAGIC GROUP BY CODE_GENDER
# MAGIC ORDER BY CUSTOMER_COUNT DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualization 3: Income Distribution Across Society

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN AMT_INCOME_TOTAL <= 100000 THEN 'Low Income (≤100K)'
# MAGIC         WHEN AMT_INCOME_TOTAL <= 200000 THEN 'Lower Middle (100K-200K)'
# MAGIC         WHEN AMT_INCOME_TOTAL <= 300000 THEN 'Middle Income (200K-300K)'
# MAGIC         WHEN AMT_INCOME_TOTAL <= 500000 THEN 'Upper Middle (300K-500K)'
# MAGIC         WHEN AMT_INCOME_TOTAL <= 1000000 THEN 'High Income (500K-1M)'
# MAGIC         ELSE 'Very High Income (1M+)'
# MAGIC     END as INCOME_BRACKET,
# MAGIC     COUNT(*) as CUSTOMER_COUNT,
# MAGIC     ROUND(AVG(AMT_INCOME_TOTAL), 0) as AVG_INCOME,
# MAGIC     ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM application_train WHERE AMT_INCOME_TOTAL IS NOT NULL), 2) as PERCENTAGE
# MAGIC FROM application_train 
# MAGIC WHERE AMT_INCOME_TOTAL IS NOT NULL AND AMT_INCOME_TOTAL > 0
# MAGIC GROUP BY 
# MAGIC     CASE 
# MAGIC         WHEN AMT_INCOME_TOTAL <= 100000 THEN 'Low Income (≤100K)'
# MAGIC         WHEN AMT_INCOME_TOTAL <= 200000 THEN 'Lower Middle (100K-200K)'
# MAGIC         WHEN AMT_INCOME_TOTAL <= 300000 THEN 'Middle Income (200K-300K)'
# MAGIC         WHEN AMT_INCOME_TOTAL <= 500000 THEN 'Upper Middle (300K-500K)'
# MAGIC         WHEN AMT_INCOME_TOTAL <= 1000000 THEN 'High Income (500K-1M)'
# MAGIC         ELSE 'Very High Income (1M+)'
# MAGIC     END
# MAGIC ORDER BY AVG_INCOME

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualization 4: Employment & Education Profile

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     NAME_EDUCATION_TYPE as EDUCATION_LEVEL,
# MAGIC     NAME_INCOME_TYPE as EMPLOYMENT_TYPE,
# MAGIC     COUNT(*) as CUSTOMER_COUNT,
# MAGIC     ROUND(AVG(AMT_INCOME_TOTAL), 0) as AVG_INCOME
# MAGIC FROM application_train 
# MAGIC WHERE NAME_EDUCATION_TYPE IS NOT NULL 
# MAGIC     AND NAME_INCOME_TYPE IS NOT NULL
# MAGIC     AND AMT_INCOME_TOTAL IS NOT NULL
# MAGIC GROUP BY NAME_EDUCATION_TYPE, NAME_INCOME_TYPE
# MAGIC HAVING COUNT(*) >= 100
# MAGIC ORDER BY EDUCATION_LEVEL, AVG_INCOME DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualization 5: Occupation Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     OCCUPATION_TYPE as JOB_CATEGORY,
# MAGIC     COUNT(*) as CUSTOMER_COUNT,
# MAGIC     ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM application_train WHERE OCCUPATION_TYPE IS NOT NULL), 2) as PERCENTAGE,
# MAGIC     ROUND(AVG(AMT_INCOME_TOTAL), 0) as AVG_INCOME
# MAGIC FROM application_train 
# MAGIC WHERE OCCUPATION_TYPE IS NOT NULL
# MAGIC GROUP BY OCCUPATION_TYPE
# MAGIC ORDER BY CUSTOMER_COUNT DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualization 6: Average Salary by Profession
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     OCCUPATION_TYPE as PROFESSION,
# MAGIC     COUNT(*) as EMPLOYEE_COUNT,
# MAGIC     ROUND(AVG(AMT_INCOME_TOTAL), 0) as AVERAGE_SALARY,
# MAGIC     ROUND(MIN(AMT_INCOME_TOTAL), 0) as MIN_SALARY,
# MAGIC     ROUND(MAX(AMT_INCOME_TOTAL), 0) as MAX_SALARY,
# MAGIC     ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY AMT_INCOME_TOTAL), 0) as MEDIAN_SALARY
# MAGIC FROM application_train 
# MAGIC WHERE OCCUPATION_TYPE IS NOT NULL 
# MAGIC     AND AMT_INCOME_TOTAL IS NOT NULL
# MAGIC     AND AMT_INCOME_TOTAL > 0
# MAGIC GROUP BY OCCUPATION_TYPE
# MAGIC HAVING COUNT(*) >= 100
# MAGIC ORDER BY AVERAGE_SALARY DESC

# COMMAND ----------

# MAGIC %md
# MAGIC #CHAPTER 2: WHAT DO OUR CUSTOMERS WANT?

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.1 Loan Application Patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ###Visualization 7: Loan Amount Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN AMT_CREDIT <= 200000 THEN 'Small Loans (≤200K)'
# MAGIC         WHEN AMT_CREDIT <= 500000 THEN 'Medium Loans (200K-500K)'
# MAGIC         WHEN AMT_CREDIT <= 1000000 THEN 'Large Loans (500K-1M)'
# MAGIC         WHEN AMT_CREDIT <= 2000000 THEN 'Very Large Loans (1M-2M)'
# MAGIC         ELSE 'Jumbo Loans (2M+)'
# MAGIC     END as LOAN_SIZE_CATEGORY,
# MAGIC     COUNT(*) as APPLICATION_COUNT,
# MAGIC     ROUND(AVG(AMT_CREDIT), 0) as AVG_LOAN_AMOUNT,
# MAGIC     ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM application_train WHERE AMT_CREDIT IS NOT NULL), 2) as PERCENTAGE
# MAGIC FROM application_train 
# MAGIC WHERE AMT_CREDIT IS NOT NULL AND AMT_CREDIT > 0
# MAGIC GROUP BY 
# MAGIC     CASE 
# MAGIC         WHEN AMT_CREDIT <= 200000 THEN 'Small Loans (≤200K)'
# MAGIC         WHEN AMT_CREDIT <= 500000 THEN 'Medium Loans (200K-500K)'
# MAGIC         WHEN AMT_CREDIT <= 1000000 THEN 'Large Loans (500K-1M)'
# MAGIC         WHEN AMT_CREDIT <= 2000000 THEN 'Very Large Loans (1M-2M)'
# MAGIC         ELSE 'Jumbo Loans (2M+)'
# MAGIC     END
# MAGIC ORDER BY AVG_LOAN_AMOUNT

# COMMAND ----------

# MAGIC %md
# MAGIC ###Visualization 8: Loan Purpose Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     NAME_CONTRACT_TYPE as LOAN_TYPE,
# MAGIC     COUNT(*) as APPLICATION_COUNT,
# MAGIC     ROUND(AVG(AMT_CREDIT), 0) as AVG_LOAN_AMOUNT,
# MAGIC     ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM application_train), 2) as PERCENTAGE
# MAGIC FROM application_train 
# MAGIC WHERE NAME_CONTRACT_TYPE IS NOT NULL
# MAGIC GROUP BY NAME_CONTRACT_TYPE
# MAGIC ORDER BY APPLICATION_COUNT DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Visualization 9: Credit-to-Income Ratio Analysis
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     AMT_INCOME_TOTAL as ANNUAL_INCOME,
# MAGIC     AMT_CREDIT as CREDIT_REQUESTED,
# MAGIC     ROUND(AMT_CREDIT / AMT_INCOME_TOTAL, 2) as CREDIT_TO_INCOME_RATIO,
# MAGIC     CASE 
# MAGIC         WHEN AMT_CREDIT / AMT_INCOME_TOTAL <= 1 THEN 'Conservative (≤1x Income)'
# MAGIC         WHEN AMT_CREDIT / AMT_INCOME_TOTAL <= 2 THEN 'Moderate (1-2x Income)'
# MAGIC         WHEN AMT_CREDIT / AMT_INCOME_TOTAL <= 3 THEN 'Aggressive (2-3x Income)'
# MAGIC         ELSE 'Very Aggressive (3x+ Income)'
# MAGIC     END as RISK_APPETITE
# MAGIC FROM application_train 
# MAGIC WHERE AMT_INCOME_TOTAL IS NOT NULL 
# MAGIC     AND AMT_CREDIT IS NOT NULL 
# MAGIC     AND AMT_INCOME_TOTAL > 0
# MAGIC     AND AMT_CREDIT > 0
# MAGIC     AND AMT_CREDIT / AMT_INCOME_TOTAL <= 10  -- Remove extreme outliers

# COMMAND ----------

# MAGIC %md
# MAGIC #CHAPTER 3: HOW DO WE DECIDE?

# COMMAND ----------

# MAGIC %md
# MAGIC ##3.1 Previous Application History Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualization 8: Application Success Rates Over Time

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     NAME_CONTRACT_STATUS as APPLICATION_STATUS,
# MAGIC     COUNT(*) as APPLICATION_COUNT,
# MAGIC     ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM previous_application), 2) as PERCENTAGE,
# MAGIC     ROUND(AVG(AMT_APPLICATION), 0) as AVG_REQUESTED_AMOUNT,
# MAGIC     ROUND(AVG(AMT_CREDIT), 0) as AVG_APPROVED_AMOUNT
# MAGIC FROM previous_application 
# MAGIC WHERE NAME_CONTRACT_STATUS IS NOT NULL
# MAGIC GROUP BY NAME_CONTRACT_STATUS
# MAGIC ORDER BY APPLICATION_COUNT DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualization 9: Application Rejection Reason

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COALESCE(CODE_REJECT_REASON, 'Not Specified') as REJECTION_REASON,
# MAGIC     COUNT(*) as REJECTION_COUNT,
# MAGIC     ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM previous_application WHERE NAME_CONTRACT_STATUS = 'Refused'), 2) as PERCENTAGE_OF_REJECTIONS,
# MAGIC     ROUND(AVG(AMT_APPLICATION), 0) as AVG_REQUESTED_AMOUNT
# MAGIC FROM previous_application 
# MAGIC WHERE NAME_CONTRACT_STATUS = 'Refused'
# MAGIC GROUP BY CODE_REJECT_REASON
# MAGIC ORDER BY REJECTION_COUNT DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualization 10: Approval Rates by Loan Size

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN AMT_APPLICATION <= 200000 THEN 'Small (≤200K)'
# MAGIC         WHEN AMT_APPLICATION <= 500000 THEN 'Medium (200K-500K)'
# MAGIC         WHEN AMT_APPLICATION <= 1000000 THEN 'Large (500K-1M)'
# MAGIC         ELSE 'Very Large (1M+)'
# MAGIC     END as APPLICATION_SIZE,
# MAGIC     NAME_CONTRACT_STATUS as STATUS,
# MAGIC     COUNT(*) as APPLICATION_COUNT,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY 
# MAGIC         CASE 
# MAGIC             WHEN AMT_APPLICATION <= 200000 THEN 'Small (≤200K)'
# MAGIC             WHEN AMT_APPLICATION <= 500000 THEN 'Medium (200K-500K)'
# MAGIC             WHEN AMT_APPLICATION <= 1000000 THEN 'Large (500K-1M)'
# MAGIC             ELSE 'Very Large (1M+)'
# MAGIC         END), 2) as PERCENTAGE
# MAGIC FROM previous_application 
# MAGIC WHERE AMT_APPLICATION IS NOT NULL 
# MAGIC     AND NAME_CONTRACT_STATUS IS NOT NULL
# MAGIC GROUP BY 
# MAGIC     CASE 
# MAGIC         WHEN AMT_APPLICATION <= 200000 THEN 'Small (≤200K)'
# MAGIC         WHEN AMT_APPLICATION <= 500000 THEN 'Medium (200K-500K)'
# MAGIC         WHEN AMT_APPLICATION <= 1000000 THEN 'Large (500K-1M)'
# MAGIC         ELSE 'Very Large (1M+)'
# MAGIC     END,
# MAGIC     NAME_CONTRACT_STATUS
# MAGIC ORDER BY APPLICATION_SIZE, STATUS

# COMMAND ----------

# MAGIC %md
# MAGIC # CHAPTER 4: WHAT'S THEIR CREDIT HISTORY?

# COMMAND ----------

# MAGIC %md
# MAGIC ##4.1 Credit Bureau Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ###Visualization 11: Credit Portfolio Overview

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CREDIT_TYPE as CREDIT_PRODUCT_TYPE,
# MAGIC     CREDIT_ACTIVE as STATUS,
# MAGIC     COUNT(*) as CREDIT_COUNT,
# MAGIC     ROUND(AVG(AMT_CREDIT_SUM), 0) as AVG_CREDIT_AMOUNT,
# MAGIC     ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM bureau), 2) as PERCENTAGE
# MAGIC FROM bureau 
# MAGIC WHERE CREDIT_TYPE IS NOT NULL 
# MAGIC     AND CREDIT_ACTIVE IS NOT NULL
# MAGIC GROUP BY CREDIT_TYPE, CREDIT_ACTIVE
# MAGIC ORDER BY CREDIT_TYPE, CREDIT_COUNT DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Visualization 12: Payment Behavior Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     bb.STATUS as PAYMENT_STATUS,
# MAGIC     COUNT(*) as OCCURRENCE_COUNT,
# MAGIC     ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM bureau_balance WHERE STATUS IS NOT NULL), 2) as PERCENTAGE,
# MAGIC     CASE 
# MAGIC         WHEN bb.STATUS = 'C' THEN 'Paid Off (Closed)'
# MAGIC         WHEN bb.STATUS = '0' THEN 'On Time (0 Days)'
# MAGIC         WHEN bb.STATUS = '1' THEN 'Late 1-30 Days'
# MAGIC         WHEN bb.STATUS >= '2' THEN 'Late 30+ Days'
# MAGIC         WHEN bb.STATUS = '3' THEN 'Late 61-90 Days'
# MAGIC         WHEN bb.STATUS = '4' THEN 'Late 91-120 Days'
# MAGIC         WHEN bb.STATUS = '5' THEN 'Late 120+ Days'
# MAGIC         ELSE 'Other Status'
# MAGIC     END as PAYMENT_DESCRIPTION
# MAGIC FROM bureau_balance bb
# MAGIC WHERE bb.STATUS IS NOT NULL
# MAGIC GROUP BY bb.STATUS
# MAGIC ORDER BY OCCURRENCE_COUNT DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Visualization 13: Credit Utilization Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN AMT_CREDIT_SUM_DEBT IS NULL OR AMT_CREDIT_SUM IS NULL OR AMT_CREDIT_SUM = 0 THEN 'No Data'
# MAGIC         WHEN AMT_CREDIT_SUM_DEBT / AMT_CREDIT_SUM <= 0.1 THEN 'Very Low (0-10%)'
# MAGIC         WHEN AMT_CREDIT_SUM_DEBT / AMT_CREDIT_SUM <= 0.3 THEN 'Low (10-30%)'
# MAGIC         WHEN AMT_CREDIT_SUM_DEBT / AMT_CREDIT_SUM <= 0.5 THEN 'Moderate (30-50%)'
# MAGIC         WHEN AMT_CREDIT_SUM_DEBT / AMT_CREDIT_SUM <= 0.7 THEN 'High (50-70%)'
# MAGIC         WHEN AMT_CREDIT_SUM_DEBT / AMT_CREDIT_SUM <= 0.9 THEN 'Very High (70-90%)'
# MAGIC         ELSE 'Maxed Out (90%+)'
# MAGIC     END as UTILIZATION_LEVEL,
# MAGIC     COUNT(*) as CUSTOMER_COUNT,
# MAGIC     ROUND(AVG(AMT_CREDIT_SUM_DEBT / AMT_CREDIT_SUM) * 100, 2) as AVG_UTILIZATION_PCT
# MAGIC FROM bureau 
# MAGIC WHERE AMT_CREDIT_SUM > 0 AND AMT_CREDIT_SUM_DEBT >= 0
# MAGIC GROUP BY 
# MAGIC     CASE 
# MAGIC         WHEN AMT_CREDIT_SUM_DEBT IS NULL OR AMT_CREDIT_SUM IS NULL OR AMT_CREDIT_SUM = 0 THEN 'No Data'
# MAGIC         WHEN AMT_CREDIT_SUM_DEBT / AMT_CREDIT_SUM <= 0.1 THEN 'Very Low (0-10%)'
# MAGIC         WHEN AMT_CREDIT_SUM_DEBT / AMT_CREDIT_SUM <= 0.3 THEN 'Low (10-30%)'
# MAGIC         WHEN AMT_CREDIT_SUM_DEBT / AMT_CREDIT_SUM <= 0.5 THEN 'Moderate (30-50%)'
# MAGIC         WHEN AMT_CREDIT_SUM_DEBT / AMT_CREDIT_SUM <= 0.7 THEN 'High (50-70%)'
# MAGIC         WHEN AMT_CREDIT_SUM_DEBT / AMT_CREDIT_SUM <= 0.9 THEN 'Very High (70-90%)'
# MAGIC         ELSE 'Maxed Out (90%+)'
# MAGIC     END
# MAGIC ORDER BY AVG_UTILIZATION_PCT

# COMMAND ----------

# MAGIC %md
# MAGIC #CHAPTER 5: HOW DO CUSTOMERS PAY?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Installment Payment Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ###Visualization 14: Payment Timeliness Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT <= -5 THEN 'Early (5+ days)'
# MAGIC         WHEN DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT <= 0 THEN 'On Time'
# MAGIC         WHEN DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT <= 5 THEN 'Slightly Late (1-5 days)'
# MAGIC         WHEN DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT <= 15 THEN 'Late (6-15 days)'
# MAGIC         WHEN DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT <= 30 THEN 'Very Late (16-30 days)'
# MAGIC         ELSE 'Severely Late (30+ days)'
# MAGIC     END as PAYMENT_TIMING,
# MAGIC     COUNT(*) as PAYMENT_COUNT,
# MAGIC     ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM installments_payments WHERE DAYS_ENTRY_PAYMENT IS NOT NULL AND DAYS_INSTALMENT IS NOT NULL), 2) as PERCENTAGE,
# MAGIC     ROUND(AVG(AMT_PAYMENT), 0) as AVG_PAYMENT_AMOUNT
# MAGIC FROM installments_payments 
# MAGIC WHERE DAYS_ENTRY_PAYMENT IS NOT NULL 
# MAGIC     AND DAYS_INSTALMENT IS NOT NULL
# MAGIC GROUP BY 
# MAGIC     CASE 
# MAGIC         WHEN DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT <= -5 THEN 'Early (5+ days)'
# MAGIC         WHEN DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT <= 0 THEN 'On Time'
# MAGIC         WHEN DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT <= 5 THEN 'Slightly Late (1-5 days)'
# MAGIC         WHEN DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT <= 15 THEN 'Late (6-15 days)'
# MAGIC         WHEN DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT <= 30 THEN 'Very Late (16-30 days)'
# MAGIC         ELSE 'Severely Late (30+ days)'
# MAGIC     END
# MAGIC ORDER BY 
# MAGIC     CASE 
# MAGIC         WHEN PAYMENT_TIMING = 'Early (5+ days)' THEN 1
# MAGIC         WHEN PAYMENT_TIMING = 'On Time' THEN 2
# MAGIC         WHEN PAYMENT_TIMING = 'Slightly Late (1-5 days)' THEN 3
# MAGIC         WHEN PAYMENT_TIMING = 'Late (6-15 days)' THEN 4
# MAGIC         WHEN PAYMENT_TIMING = 'Very Late (16-30 days)' THEN 5
# MAGIC         ELSE 6
# MAGIC     END

# COMMAND ----------

# MAGIC %md
# MAGIC ###Visualization 15: Payment Amount vs Expected Amount

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     AMT_INSTALMENT as EXPECTED_PAYMENT,
# MAGIC     AMT_PAYMENT as ACTUAL_PAYMENT,
# MAGIC     ROUND((AMT_PAYMENT - AMT_INSTALMENT) / AMT_INSTALMENT * 100, 2) as PAYMENT_VARIANCE_PCT,
# MAGIC     CASE 
# MAGIC         WHEN AMT_PAYMENT / AMT_INSTALMENT >= 1.1 THEN 'Overpaid (10%+)'
# MAGIC         WHEN AMT_PAYMENT / AMT_INSTALMENT >= 1.05 THEN 'Slightly Overpaid (5-10%)'
# MAGIC         WHEN AMT_PAYMENT / AMT_INSTALMENT >= 0.95 THEN 'Paid as Expected'
# MAGIC         WHEN AMT_PAYMENT / AMT_INSTALMENT >= 0.9 THEN 'Slightly Underpaid (5-10%)'
# MAGIC         ELSE 'Significantly Underpaid (10%+)'
# MAGIC     END as PAYMENT_CATEGORY
# MAGIC FROM installments_payments 
# MAGIC WHERE AMT_INSTALMENT IS NOT NULL 
# MAGIC     AND AMT_PAYMENT IS NOT NULL 
# MAGIC     AND AMT_INSTALMENT > 0
# MAGIC     AND AMT_PAYMENT > 0
# MAGIC     AND AMT_INSTALMENT <= 100000  -- Remove outliers

# COMMAND ----------

# MAGIC %md
# MAGIC #CHAPTER 6: THE FINAL ANSWER - WHO DEFAULTS?

# COMMAND ----------

# MAGIC %md
# MAGIC ##6.1 Comprehensive Risk Analysis (Joined Data)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Preparation for Joined Analysis

# COMMAND ----------

# Create master dataset with all relevant information
df_master = spark.sql("""
WITH app_features AS (
    SELECT 
        SK_ID_CURR,
        TARGET,
        CODE_GENDER,
        FLOOR(ABS(DAYS_BIRTH)/365.25) as AGE,
        CASE 
            WHEN FLOOR(ABS(DAYS_BIRTH)/365.25) BETWEEN 18 AND 30 THEN 'Young (18-30)'
            WHEN FLOOR(ABS(DAYS_BIRTH)/365.25) BETWEEN 31 AND 45 THEN 'Prime (31-45)'
            WHEN FLOOR(ABS(DAYS_BIRTH)/365.25) BETWEEN 46 AND 60 THEN 'Mature (46-60)'
            ELSE 'Senior (60+)'
        END as AGE_GROUP,
        AMT_INCOME_TOTAL,
        AMT_CREDIT,
        AMT_ANNUITY,
        NAME_CONTRACT_TYPE,
        NAME_INCOME_TYPE,
        NAME_EDUCATION_TYPE,
        NAME_FAMILY_STATUS,
        CNT_CHILDREN,
        ROUND(AMT_CREDIT / AMT_INCOME_TOTAL, 2) as CREDIT_INCOME_RATIO,
        CASE 
            WHEN AMT_INCOME_TOTAL <= 150000 THEN 'Low Income'
            WHEN AMT_INCOME_TOTAL <= 300000 THEN 'Middle Income'
            ELSE 'High Income'
        END as INCOME_CATEGORY
    FROM application_train
    WHERE AMT_INCOME_TOTAL IS NOT NULL AND AMT_CREDIT IS NOT NULL
),
bureau_features AS (
    SELECT 
        SK_ID_CURR,
        COUNT(SK_ID_BUREAU) as TOTAL_BUREAU_CREDITS,
        SUM(CASE WHEN CREDIT_ACTIVE = 'Active' THEN 1 ELSE 0 END) as ACTIVE_CREDITS,
        SUM(CASE WHEN CREDIT_ACTIVE = 'Closed' THEN 1 ELSE 0 END) as CLOSED_CREDITS,
        COALESCE(SUM(AMT_CREDIT_SUM_DEBT), 0) as TOTAL_DEBT,
        COALESCE(AVG(AMT_CREDIT_SUM), 0) as AVG_CREDIT,
        MAX(DAYS_CREDIT) as DAYS_LAST_CREDIT
    FROM bureau
    GROUP BY SK_ID_CURR
),
previous_features AS (
    SELECT 
        SK_ID_CURR,
        COUNT(SK_ID_PREV) as TOTAL_PREV_APPLICATIONS,
        SUM(CASE WHEN NAME_CONTRACT_STATUS = 'Approved' THEN 1 ELSE 0 END) as APPROVED_APPLICATIONS,
        SUM(CASE WHEN NAME_CONTRACT_STATUS = 'Refused' THEN 1 ELSE 0 END) as REFUSED_APPLICATIONS,
        CASE 
            WHEN COUNT(SK_ID_PREV) = 0 THEN 0
            ELSE ROUND(SUM(CASE WHEN NAME_CONTRACT_STATUS = 'Approved' THEN 1 ELSE 0 END) * 100.0 / COUNT(SK_ID_PREV), 2)
        END as APPROVAL_RATE
    FROM previous_application
    GROUP BY SK_ID_CURR
)
SELECT 
    a.*,
    COALESCE(b.TOTAL_BUREAU_CREDITS, 0) as BUREAU_CREDITS,
    COALESCE(b.ACTIVE_CREDITS, 0) as ACTIVE_CREDITS,
    COALESCE(b.TOTAL_DEBT, 0) as TOTAL_DEBT,
    COALESCE(p.TOTAL_PREV_APPLICATIONS, 0) as PREV_APPLICATIONS,
    COALESCE(p.APPROVAL_RATE, 0) as HISTORICAL_APPROVAL_RATE
FROM app_features a
LEFT JOIN bureau_features b ON a.SK_ID_CURR = b.SK_ID_CURR
LEFT JOIN previous_features p ON a.SK_ID_CURR = p.SK_ID_CURR
""")

df_master.createOrReplaceTempView("master_risk_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Visualization 16: The Default Rate Dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     'OVERALL_DEFAULT_RATE' as METRIC,
# MAGIC     ROUND(AVG(TARGET) * 100, 2) as VALUE,
# MAGIC     '%' as UNIT,
# MAGIC     COUNT(*) as TOTAL_CUSTOMERS
# MAGIC FROM master_risk_data
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'AVG_LOAN_AMOUNT' as METRIC,
# MAGIC     ROUND(AVG(AMT_CREDIT), 0) as VALUE,
# MAGIC     'USD' as UNIT,
# MAGIC     COUNT(*) as TOTAL_CUSTOMERS
# MAGIC FROM master_risk_data
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'AVG_INCOME' as METRIC,
# MAGIC     ROUND(AVG(AMT_INCOME_TOTAL), 0) as VALUE,
# MAGIC     'USD' as UNIT,
# MAGIC     COUNT(*) as TOTAL_CUSTOMERS
# MAGIC FROM master_risk_data
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'AVG_CREDIT_INCOME_RATIO' as METRIC,
# MAGIC     ROUND(AVG(CREDIT_INCOME_RATIO), 2) as VALUE,
# MAGIC     'X' as UNIT,
# MAGIC     COUNT(*) as TOTAL_CUSTOMERS
# MAGIC FROM master_risk_data

# COMMAND ----------

# MAGIC %md
# MAGIC ###Visualization 17: Risk Segmentation Matrix

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT 
# MAGIC     AGE_GROUP,
# MAGIC     INCOME_CATEGORY,
# MAGIC     COUNT(*) as CUSTOMER_COUNT,
# MAGIC     ROUND(AVG(TARGET) * 100, 2) as DEFAULT_RATE,
# MAGIC     ROUND(AVG(AMT_CREDIT), 0) as AVG_LOAN_SIZE,
# MAGIC     ROUND(AVG(CREDIT_INCOME_RATIO), 2) as AVG_CREDIT_RATIO
# MAGIC FROM master_risk_data
# MAGIC GROUP BY AGE_GROUP, INCOME_CATEGORY
# MAGIC HAVING COUNT(*) >= 100
# MAGIC ORDER BY DEFAULT_RATE DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Visualization 18: Credit History Impact on Default

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN BUREAU_CREDITS = 0 THEN 'No Credit History'
# MAGIC         WHEN BUREAU_CREDITS <= 2 THEN 'Limited History (1-2)'
# MAGIC         WHEN BUREAU_CREDITS <= 5 THEN 'Moderate History (3-5)'
# MAGIC         WHEN BUREAU_CREDITS <= 10 THEN 'Extensive History (6-10)'
# MAGIC         ELSE 'Very Extensive (10+)'
# MAGIC     END as CREDIT_HISTORY_LEVEL,
# MAGIC     ROUND(AVG(TARGET) * 100, 2) as DEFAULT_RATE,
# MAGIC     COUNT(*) as CUSTOMER_COUNT,
# MAGIC     ROUND(AVG(AMT_CREDIT), 0) as AVG_LOAN_AMOUNT,
# MAGIC     ROUND(AVG(ACTIVE_CREDITS), 1) as AVG_ACTIVE_CREDITS
# MAGIC FROM master_risk_data
# MAGIC GROUP BY 
# MAGIC     CASE 
# MAGIC         WHEN BUREAU_CREDITS = 0 THEN 'No Credit History'
# MAGIC         WHEN BUREAU_CREDITS <= 2 THEN 'Limited History (1-2)'
# MAGIC         WHEN BUREAU_CREDITS <= 5 THEN 'Moderate History (3-5)'
# MAGIC         WHEN BUREAU_CREDITS <= 10 THEN 'Extensive History (6-10)'
# MAGIC         ELSE 'Very Extensive (10+)'
# MAGIC     END
# MAGIC ORDER BY DEFAULT_RATE DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Visualization 19: The Risk Prediction Model Insights

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     ROUND(CREDIT_INCOME_RATIO, 1) as CREDIT_TO_INCOME_RATIO,
# MAGIC     ROUND(AVG(CASE WHEN BUREAU_CREDITS = 0 THEN TARGET END) * 100, 2) as NO_HISTORY_DEFAULT_RATE,
# MAGIC     ROUND(AVG(CASE WHEN BUREAU_CREDITS BETWEEN 1 AND 5 THEN TARGET END) * 100, 2) as SOME_HISTORY_DEFAULT_RATE,
# MAGIC     ROUND(AVG(CASE WHEN BUREAU_CREDITS > 5 THEN TARGET END) * 100, 2) as EXTENSIVE_HISTORY_DEFAULT_RATE,
# MAGIC     COUNT(*) as TOTAL_CUSTOMERS
# MAGIC FROM master_risk_data
# MAGIC WHERE CREDIT_INCOME_RATIO BETWEEN 0.5 AND 5.0  -- Focus on reasonable range
# MAGIC GROUP BY ROUND(CREDIT_INCOME_RATIO, 1)
# MAGIC HAVING COUNT(*) >= 50
# MAGIC ORDER BY CREDIT_TO_INCOME_RATIO

# COMMAND ----------

# MAGIC %md
# MAGIC ###Visualization 20: Education & Employment Risk Profile

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     NAME_EDUCATION_TYPE as EDUCATION,
# MAGIC     NAME_INCOME_TYPE as EMPLOYMENT,
# MAGIC     COUNT(*) as CUSTOMER_COUNT,
# MAGIC     ROUND(AVG(TARGET) * 100, 2) as DEFAULT_RATE,
# MAGIC     ROUND(AVG(AMT_INCOME_TOTAL), 0) as AVG_INCOME,
# MAGIC     ROUND(AVG(AMT_CREDIT), 0) as AVG_LOAN_SIZE
# MAGIC FROM master_risk_data
# MAGIC WHERE NAME_EDUCATION_TYPE IS NOT NULL 
# MAGIC     AND NAME_INCOME_TYPE IS NOT NULL
# MAGIC GROUP BY NAME_EDUCATION_TYPE, NAME_INCOME_TYPE
# MAGIC HAVING COUNT(*) >= 100
# MAGIC ORDER BY DEFAULT_RATE DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Visualization 21: Family Structure & Financial Behavior

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     NAME_FAMILY_STATUS as FAMILY_STATUS,
# MAGIC     CASE 
# MAGIC         WHEN CNT_CHILDREN = 0 THEN 'No Children'
# MAGIC         WHEN CNT_CHILDREN = 1 THEN '1 Child'
# MAGIC         WHEN CNT_CHILDREN = 2 THEN '2 Children'
# MAGIC         ELSE '3+ Children'
# MAGIC     END as CHILDREN_CATEGORY,
# MAGIC     COUNT(*) as CUSTOMER_COUNT,
# MAGIC     ROUND(AVG(TARGET) * 100, 2) as DEFAULT_RATE,
# MAGIC     ROUND(AVG(AMT_INCOME_TOTAL), 0) as AVG_INCOME,
# MAGIC     ROUND(AVG(CREDIT_INCOME_RATIO), 2) as AVG_CREDIT_RATIO
# MAGIC FROM master_risk_data
# MAGIC WHERE NAME_FAMILY_STATUS IS NOT NULL
# MAGIC GROUP BY NAME_FAMILY_STATUS, 
# MAGIC     CASE 
# MAGIC         WHEN CNT_CHILDREN = 0 THEN 'No Children'
# MAGIC         WHEN CNT_CHILDREN = 1 THEN '1 Child'
# MAGIC         WHEN CNT_CHILDREN = 2 THEN '2 Children'
# MAGIC         ELSE '3+ Children'
# MAGIC     END
# MAGIC HAVING COUNT(*) >= 50
# MAGIC ORDER BY FAMILY_STATUS, DEFAULT_RATE DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Visualization 22: Previous Application Success vs Current Risk

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN PREV_APPLICATIONS = 0 THEN 'First Time Customer'
# MAGIC         WHEN PREV_APPLICATIONS = 1 THEN '1 Previous Application'
# MAGIC         WHEN PREV_APPLICATIONS BETWEEN 2 AND 3 THEN '2-3 Previous Applications'
# MAGIC         WHEN PREV_APPLICATIONS BETWEEN 4 AND 6 THEN '4-6 Previous Applications'
# MAGIC         ELSE '7+ Previous Applications'
# MAGIC     END as APPLICATION_HISTORY,
# MAGIC     ROUND(AVG(TARGET) * 100, 2) as DEFAULT_RATE,
# MAGIC     ROUND(AVG(HISTORICAL_APPROVAL_RATE), 2) as AVG_HISTORICAL_APPROVAL_RATE,
# MAGIC     COUNT(*) as CUSTOMER_COUNT,
# MAGIC     ROUND(AVG(AMT_CREDIT), 0) as AVG_CURRENT_LOAN
# MAGIC FROM master_risk_data
# MAGIC GROUP BY 
# MAGIC     CASE 
# MAGIC         WHEN PREV_APPLICATIONS = 0 THEN 'First Time Customer'
# MAGIC         WHEN PREV_APPLICATIONS = 1 THEN '1 Previous Application'
# MAGIC         WHEN PREV_APPLICATIONS BETWEEN 2 AND 3 THEN '2-3 Previous Applications'
# MAGIC         WHEN PREV_APPLICATIONS BETWEEN 4 AND 6 THEN '4-6 Previous Applications'
# MAGIC         ELSE '7+ Previous Applications'
# MAGIC     END
# MAGIC HAVING COUNT(*) >= 100
# MAGIC ORDER BY AVG(PREV_APPLICATIONS)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Visualization 23: The Ultimate Risk Score Dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH risk_scores AS (
# MAGIC     SELECT 
# MAGIC         SK_ID_CURR,
# MAGIC         TARGET,
# MAGIC         -- Age Risk (younger = higher risk)
# MAGIC         CASE 
# MAGIC             WHEN AGE <= 25 THEN 3
# MAGIC             WHEN AGE <= 35 THEN 2
# MAGIC             WHEN AGE <= 50 THEN 1
# MAGIC             ELSE 0
# MAGIC         END as AGE_RISK_SCORE,
# MAGIC         
# MAGIC         -- Income Risk (lower income = higher risk)
# MAGIC         CASE 
# MAGIC             WHEN AMT_INCOME_TOTAL <= 100000 THEN 3
# MAGIC             WHEN AMT_INCOME_TOTAL <= 200000 THEN 2
# MAGIC             WHEN AMT_INCOME_TOTAL <= 400000 THEN 1
# MAGIC             ELSE 0
# MAGIC         END as INCOME_RISK_SCORE,
# MAGIC         
# MAGIC         -- Credit Ratio Risk (higher ratio = higher risk)
# MAGIC         CASE 
# MAGIC             WHEN CREDIT_INCOME_RATIO >= 3 THEN 3
# MAGIC             WHEN CREDIT_INCOME_RATIO >= 2 THEN 2
# MAGIC             WHEN CREDIT_INCOME_RATIO >= 1.5 THEN 1
# MAGIC             ELSE 0
# MAGIC         END as CREDIT_RATIO_RISK_SCORE,
# MAGIC         
# MAGIC         -- Credit History Risk (no history = higher risk)
# MAGIC         CASE 
# MAGIC             WHEN BUREAU_CREDITS = 0 THEN 3
# MAGIC             WHEN BUREAU_CREDITS <= 2 THEN 2
# MAGIC             WHEN BUREAU_CREDITS <= 5 THEN 1
# MAGIC             ELSE 0
# MAGIC         END as CREDIT_HISTORY_RISK_SCORE,
# MAGIC         
# MAGIC         -- Previous Application Risk
# MAGIC         CASE 
# MAGIC             WHEN PREV_APPLICATIONS = 0 THEN 1
# MAGIC             WHEN HISTORICAL_APPROVAL_RATE < 50 THEN 3
# MAGIC             WHEN HISTORICAL_APPROVAL_RATE < 80 THEN 2
# MAGIC             ELSE 0
# MAGIC         END as PREV_APP_RISK_SCORE
# MAGIC     FROM master_risk_data
# MAGIC ),
# MAGIC final_scores AS (
# MAGIC     SELECT 
# MAGIC         *,
# MAGIC         (AGE_RISK_SCORE + INCOME_RISK_SCORE + CREDIT_RATIO_RISK_SCORE + 
# MAGIC          CREDIT_HISTORY_RISK_SCORE + PREV_APP_RISK_SCORE) as TOTAL_RISK_SCORE,
# MAGIC         CASE 
# MAGIC             WHEN (AGE_RISK_SCORE + INCOME_RISK_SCORE + CREDIT_RATIO_RISK_SCORE + 
# MAGIC                   CREDIT_HISTORY_RISK_SCORE + PREV_APP_RISK_SCORE) <= 3 THEN 'Low Risk'
# MAGIC             WHEN (AGE_RISK_SCORE + INCOME_RISK_SCORE + CREDIT_RATIO_RISK_SCORE + 
# MAGIC                   CREDIT_HISTORY_RISK_SCORE + PREV_APP_RISK_SCORE) <= 6 THEN 'Medium Risk'
# MAGIC             WHEN (AGE_RISK_SCORE + INCOME_RISK_SCORE + CREDIT_RATIO_RISK_SCORE + 
# MAGIC                   CREDIT_HISTORY_RISK_SCORE + PREV_APP_RISK_SCORE) <= 9 THEN 'High Risk'
# MAGIC             ELSE 'Very High Risk'
# MAGIC         END as RISK_CATEGORY
# MAGIC     FROM risk_scores
# MAGIC )
# MAGIC SELECT 
# MAGIC     RISK_CATEGORY,
# MAGIC     COUNT(*) as CUSTOMER_COUNT,
# MAGIC     ROUND(AVG(TARGET) * 100, 2) as ACTUAL_DEFAULT_RATE,
# MAGIC     ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM final_scores), 2) as PERCENTAGE_OF_PORTFOLIO,
# MAGIC     ROUND(AVG(TOTAL_RISK_SCORE), 1) as AVG_RISK_SCORE
# MAGIC FROM final_scores
# MAGIC GROUP BY RISK_CATEGORY
# MAGIC ORDER BY AVG_RISK_SCORE

# COMMAND ----------

# MAGIC %md
# MAGIC #EXECUTIVE SUMMARY DASHBOARD

# COMMAND ----------

# MAGIC %md
# MAGIC ##Visualization 24: Business Impact Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH business_metrics AS (
# MAGIC     SELECT 
# MAGIC         COUNT(*) as TOTAL_CUSTOMERS,
# MAGIC         SUM(AMT_CREDIT) as TOTAL_CREDIT_ISSUED,
# MAGIC         SUM(CASE WHEN TARGET = 1 THEN AMT_CREDIT ELSE 0 END) as CREDIT_AT_RISK,
# MAGIC         AVG(TARGET) as DEFAULT_RATE,
# MAGIC         COUNT(CASE WHEN BUREAU_CREDITS = 0 THEN 1 END) as NEW_CUSTOMERS,
# MAGIC         COUNT(CASE WHEN BUREAU_CREDITS > 0 THEN 1 END) as EXISTING_CUSTOMERS
# MAGIC     FROM master_risk_data
# MAGIC )
# MAGIC SELECT 
# MAGIC     'Total Portfolio Value' as METRIC,
# MAGIC     FORMAT_NUMBER(TOTAL_CREDIT_ISSUED, 0) as VALUE,
# MAGIC     'Credit issued to all customers' as DESCRIPTION
# MAGIC FROM business_metrics
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Credit at Risk' as METRIC,
# MAGIC     FORMAT_NUMBER(CREDIT_AT_RISK, 0) as VALUE,
# MAGIC     'Credit issued to customers who defaulted' as DESCRIPTION
# MAGIC FROM business_metrics
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Overall Default Rate' as METRIC,
# MAGIC     CONCAT(ROUND(DEFAULT_RATE * 100, 2), '%') as VALUE,
# MAGIC     'Percentage of customers who defaulted' as DESCRIPTION
# MAGIC FROM business_metrics
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'New vs Existing Split' as METRIC,
# MAGIC     CONCAT(ROUND(NEW_CUSTOMERS * 100.0 / (NEW_CUSTOMERS + EXISTING_CUSTOMERS), 1), '% New') as VALUE,
# MAGIC     'First-time customers vs customers with credit history' as DESCRIPTION
# MAGIC FROM business_metrics