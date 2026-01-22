SILVER LAYER

%scala

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

// 1. Read Bronze transactions as a stream
val bronzeTxnStream = spark.readStream
  .format("delta")
  .table("fraud_detection.bronze_transactions")

// 2. Read customers and merchants as static reference data
val bronzeCustDF = spark.read.format("delta")
  .table("fraud_detection.bronze_customers")
  .select("customer_id", "customer_name", "customer_city", "device_id")

val bronzeMerchDF = spark.read.format("delta")
  .table("fraud_detection.bronze_merchants")
  .select("merchant_id", "merchant_name", "merchant_category", "merchant_city")

// 3. Alias tables
val txnAlias = bronzeTxnStream.alias("txn")
val custAlias = bronzeCustDF.alias("cust")
val merchAlias = bronzeMerchDF.alias("merch")

// 4. Join and enrich data with explicit column selection
val silverStream = txnAlias
  .join(custAlias, Seq("customer_id"), "left")
  .join(merchAlias, Seq("merchant_id"), "left")
  
.select(
    col("txn.transaction_id"),
    col("txn.customer_id"),
    col("txn.merchant_id"),
    col("txn.timestamp"),
    col("txn.amount"),
    col("txn.transaction_type"),
    col("txn.transaction_city"),
    col("txn.device_id").alias("txn_device_id"),
    col("txn.is_fraud"), 
    col("cust.customer_name"),
    col("cust.customer_city"),
    col("cust.device_id").alias("cust_device_id"),
    col("merch.merchant_name"),
    col("merch.merchant_category"),
    col("merch.merchant_city")
)

  .withColumn("transaction_hour", hour(col("timestamp")))
  .withColumn("transaction_day", dayofweek(col("timestamp")))
  .withColumn("normalized_amount", col("amount") / 1000)
  .withColumn("geo_mismatch", when(col("transaction_city") =!= col("customer_city"), lit(1)).otherwise(lit(0)))
  .withColumn("is_weekend", when(col("transaction_day").isin(1,7), lit(1)).otherwise(lit(0)))
  .withColumn("is_high_risk_type", when(col("transaction_type") === "international", lit(1)).otherwise(lit(0)))
  .withColumn("is_high_risk_merchant", when(col("merchant_category") === "gambling", lit(1)).otherwise(lit(0)))

// 5. Write Silver as streaming Delta table with schema evolution enabled


silverStream.writeStream
  .format("delta")
  .option("checkpointLocation", "/mnt/checkpoints/silver_transactions_v4")
  .option("mergeSchema", "true")
  .outputMode("append")
  .queryName("silver_txn_stream")
  .trigger(Trigger.ProcessingTime("10 seconds")) // continuous streaming
  .table("fraud_detection.silver_transactions")

