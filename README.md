Sure! Here's a README file focused solely on the data engineering process for your motorcycle sales analysis project:

---

# 🚀 Motorcycle Sales Data Engineering

Welcome to the repository for the "Motorcycle Sales Data Engineering" project! This project focuses on the data engineering processes required to gather, process, and organize two-wheeler sales data from 2019 to 2023 using Apache Spark and AWS.

## 📊 Project Overview

This project aims to:
- **Import** sales data from various sources.
- **Process** and **transform** the data into a consistent format.
- **Store** the processed data in AWS S3 for further analysis.

## 🛠️ Technologies Used

- **Apache Spark**: Big data processing.
- **AWS S3**: Cloud storage.
- **Snowflake**: Data storage and retrieval.
- **Scala**: Data processing language.

## 🔍 Procedure Followed

![Data Pipeline](https://github.com/rkk96/Projectdatapipeline/assets/166745361/6755652e-2c1d-4257-bf90-fbb02ca04c4e)

### Step 1: Import Necessary Packages

```scala
spark-shell --packages net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql
import org.apache.spark.sql.functions.upper
import org.apache.spark.sql.catalyst.expressions.Upper
import org.apache.spark.sql.expressions.Window
import scala.io.Source
import scala.io._
import org.apache.spark.sql.streaming.DataStreamReader
import net.snowflake
```

### Step 2: Import and Process Data

#### First Year Sales

```scala
val firstyearsales = spark.read.format("net.snowflake.spark.snowflake")
    .option("sfURL", "https://efvwdqe-wj90521.snowflakecomputing.com")
    .option("sfAccount", "PJ12363")
    .option("sfUser", "RKK666")
    .option("sfPassword", "1+1Rahul")
    .option("sfDatabase", "RKKDB")
    .option("sfSchema", "RKKSCHEMA")
    .option("sfRole", "ACCOUNTADMIN")
    .option("sfWarehouse", "COMPUTE_WH")
    .option("dbtable", "first").load()
firstyearsales.show()
firstyearsales.printSchema()

val aggdf = firstyearsales.groupBy("MAKER").agg(sum("TOTAL").cast(IntegerType).as("total year sales"))
aggdf.write.format("parquet").option("header", "true").option("multiline", "true").mode("overwrite").save("s3://projbuck2/total_sales_by_maker.parquet")
firstyearsales.write.format("parquet").option("header", "true").option("multiline", "true").mode("overwrite").save("s3://processedproj/firstyearsales.parquet")
```

#### Second Year Sales

```scala
val secondyearsales = Source.fromURL("https://mocki.io/v1/93e92677-daa3-4681-91be-8e2b44b85a6e").mkString
val rdd = sc.parallelize(List(secondyearsales))
val df = spark.read.option("multiline", "true").json(rdd).select("SN0", "MONTH", "YEAR", "Maker", "ELECTRIC(BOV)", "PETROL", "TOTAL")
df.show()
df.printSchema()
df.write.format("parquet").option("header", "true").option("multiline", "true").mode("overwrite").save("s3://processedproj/secondyearsales.parquet")
```

#### Third Year Sales

```scala
val thirdyearsales = Source.fromURL("https://mocki.io/v1/1f16b228-f5c5-41ae-ae84-2b1132ac3557").mkString
val rdd1 = sc.parallelize(List(thirdyearsales))
val df1 = spark.read.option("multiline", "true").json(rdd1).select("SN0", "MONTH", "YEAR", "Maker", "ELECTRIC(BOV)", "PETROL", "TOTAL")
df1.show()
df1.printSchema()
df1.write.format("parquet").option("header", "true").option("multiline", "true").mode("overwrite").save("s3://processedproj/thirdyearsales.parquet")
```

#### Fourth Year Sales

```scala
val fourthyearsales = Source.fromURL("https://mocki.io/v1/5c7667ea-eaa1-4406-bffa-a531d6e9e39c").mkString
val rdd2 = sc.parallelize(List(fourthyearsales))
val df2 = spark.read.option("multiline", "true").json(rdd2).select("SN0", "MONTH", "YEAR", "Maker", "ELECTRIC(BOV)", "PETROL", "TOTAL")
df2.show()
df2.printSchema()
df2.write.format("parquet").option("header", "true").option("multiline", "true").mode("overwrite").save("s3://processedproj/fourthyearsales.parquet")
```

#### Final Year Sales

```scala
val finalyearsales = spark.read.format("parquet").option("multiline", "true").option("header", "true").load("s3://salesdata11/sales2023.parquet").select("SN0", "MONTH", "YEAR", "Maker", "ELECTRIC(BOV)", "PETROL", "TOTAL")
finalyearsales.show()
finalyearsales.printSchema()

val aggdf1 = finalyearsales.groupBy("MAKER").agg(sum("TOTAL").cast(IntegerType).as("total year sales"))
aggdf1.write.format("parquet").option("header", "true").option("multiline", "true").mode("overwrite").save("s3://projbuck2/totalsalesbymaker4.parquet")
finalyearsales.write.format("parquet").option("header", "true").option("multiline", "true").mode("overwrite").save("s3://processedproj/finalyearsales.parquet")
```

### Step 3: Combine and Save Data

```scala
val table1 = spark.read.format("parquet").option("multiline", "true").option("header", "true").load("s3://processedproj/firstyearsales.parquet")
table1.show()

val table2 = spark.read.format("parquet").option("multiline", "true").option("header", "true").load("s3://processedproj/secondyearsales.parquet")
table2.show()

val table3 = spark.read.format("parquet").option("multiline", "true").option("header", "true").load("s3://processedproj/thirdyearsales.parquet")
table3.show()

val table4 = spark.read.format("parquet").option("multiline", "true").option("header", "true").load("s3://processedproj/fourthyearsales.parquet")
table4.show()

val table5 = spark.read.format("parquet").option("multiline", "true").option("header", "true").load("s3://processedproj/finalyearsales.parquet")
table5.show()

val combinedDF = table1.union(table2).union(table3).union(table4).union(table5)
combinedDF.show()

combinedDF.write.format("parquet").option("header", "true").option("multiline", "true").mode("overwrite").save("s3://totalsales/totalsales.parquet")

combinedDF.coalesce(1).write.format("net.snowflake.spark.snowflake")
    .option("sfURL", "https://efvwdqe-wj90521.snowflakecomputing.com")
    .option("sfAccount", "PJ12363")
    .option("sfUser", "RKK666")
    .option("sfPassword", "1+1Rahul")
    .option("sfDatabase", "RKKDB")
    .option("sfSchema", "RKKSCHEMA")
    .option("sfRole", "ACCOUNTADMIN")
    .option("sfWarehouse", "COMPUTE_WH")
    .option("dbtable", "totalsales").mode("overwrite").save()
```

## 🚀 Getting Started

### Prerequisites

- Apache Spark
- AWS account
- Snowflake account

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/motorcycle-sales-data-engineering.git
   cd motorcycle-sales-data-engineering
   ```

2. Set up your Snowflake and AWS connections in the script.

### Running the Data Engineering Process

1. Open a Spark shell with the necessary packages:
   ```bash
   spark-shell --packages net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4
   ```

2. Execute the Scala scripts provided in the README to process and combine the sales data.

