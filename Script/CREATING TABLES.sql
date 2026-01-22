CREATING TABLES

%sql
CREATE TABLE IF NOT EXISTS fraud_detection.bronze_transactions
USING DELTA
LOCATION '/delta/bronze/transactions';

%sql
CREATE TABLE IF NOT EXISTS fraud_detection.bronze_customers
USING DELTA
LOCATION '/delta/bronze/customers';

%sql
CREATE TABLE IF NOT EXISTS fraud_detection.bronze_merchants
USING DELTA
LOCATION '/delta/bronze/merchants';

%sql

CREATE TABLE IF NOT EXISTS silver_transactions (
  transaction_id STRING,
  customer_id STRING,
  merchant_id STRING,
  timestamp TIMESTAMP,
  amount DOUBLE,
  transaction_type STRING,
  transaction_city STRING,
  device_id STRING,
  is_fraud INT,
  transaction_date DATE
) USING DELTA LOCATION '/delta/silver/transactions';


%sql

CREATE TABLE IF NOT EXISTS silver_customers (
  customer_id STRING,
  customer_name STRING,
  city STRING,
  device_id STRING
) USING DELTA LOCATION '/delta/silver/customers'


%sql

CREATE TABLE IF NOT EXISTS silver_merchants (
  merchant_id STRING,
  merchant_name STRING,
  merchant_category STRING,
  city STRING
) USING DELTA LOCATION '/delta/silver/merchants'


%sql

-- Create schema (database) inside hive_metastore
CREATE DATABASE IF NOT EXISTS fraud_detection;

%sql

USE fraud_detection;


%sql

CREATE TABLE IF NOT EXISTS fraud_detection.silver_transactions (
  transaction_id STRING,
  customer_id STRING,
  merchant_id STRING,
  timestamp TIMESTAMP,
  amount DOUBLE,
  transaction_type STRING,
  transaction_city STRING,
  device_id STRING,
  is_fraud INT,
  transaction_date DATE
) USING DELTA LOCATION '/delta/silver/transactions';


%sql
CREATE TABLE IF NOT EXISTS fraud_detection.silver_customers (
  customer_id STRING,
  customer_name STRING,
   city STRING,
  device_id STRING
) USING DELTA LOCATION '/delta/silver/customers';


%sql
CREATE TABLE IF NOT EXISTS fraud_detection.silver_merchants (
  merchant_id STRING,
  merchant_name STRING,
  merchant_category STRING,
  city STRING);

  
