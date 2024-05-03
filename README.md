# Complete End to End Work on Motorcycle sales 



## Problem Statement

Understanding the Performance and Future Prospects of Two-Wheeler Sales: 2019-2023

We aim to analyze sales data encompassing electric and petrol two-wheelers over five years (2019-2023). The objective is to identify performance patterns, pinpoint areas of underperformance, and forecast future trends for specific brands based on their historical sales performance. This analysis will enable informed decision-making and strategic planning to enhance the overall sales trajectory and competitiveness of these vehicles in the market.


### Overview


"Revolutionizing Two-Wheeler Sales Analysis: Leveraging Cloud Data Engineering and Machine Learning for Future Insights

Embark on a transformative journey with us as we harness the power of cloud data engineering platforms to gather, organize, and unlock the potential of vast two-wheeler sales data spanning from 2019 to 2023. Our cutting-edge approach ensures seamless integration of diverse data sources, enabling a comprehensive understanding of market dynamics and brand performance.

Through sophisticated machine learning models, we transcend traditional analysis to predict future trends with unparalleled accuracy. Delve deep into the intricacies of each brand's trajectory, uncovering hidden patterns and opportunities for growth. Our predictive analytics not only anticipate market shifts but also empower stakeholders to make informed decisions and stay ahead of the curve.

But we don't stop there. We bring the data to life with captivating visualizations on a dynamic Power BI dashboard. From sleek graphs depicting sales trends to interactive charts showcasing predictive models, our dashboard offers a user-friendly interface that simplifies complex insights for effortless interpretation.

Join us on this groundbreaking journey as we redefine the landscape of two-wheeler sales analysis, driving innovation and unlocking the full potential of your data. Together, we'll pave the way for a future of informed decision-making and unparalleled success in the dynamic world of automotive sales."




### Step by step Procedure



We have the sales data of each year in different locations and different file formats.

Now we will do all that step by step as per the client requirement.

We will be using cloud platforms to organize the code. In this case, we are going to bring the data from different sources. We are going to use AWS EMR services for our requirement.



- Step 1 : Import all the necessary packages to do our tasks

spark-shell --packages net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4(Packages for importing datas from snowflake)

-import org.apache.spark._

-import org.apache.spark.SparkContext

-import org.apache.spark.sql.SparkSession

-import org.apache.spark.sql.types._

-import org.apache.spark.sql.types.IntegerType

-import org.apache.spark.sql.functions._

-import org.apache.spark.sql

-import org.apache.spark.sql.functions.upper

-import org.apache.spark.sql.catalyst.expressions.Upper

-import org.apache.spark.sql.expressions.Window

-import scala.io.Source

-import scala.io._

-import org.apache.spark.sql.streaming.DataStreamReader

-import net.snowflake






- Step 2 :Importing all the data and converting them into a proper dataframe for analysis, then writing it back to AWS S3 for easier access. Additionally, fulfilling client requests for specific requirements regarding sales data from the first and last years.



    val firstyearsales = spark.read.format("net.snowflake.spark.snowflake").option("sfURL", "https://efvwdqe-wj90521.snowflakecomputing.com") .option("sfAccount", "PJ12363").option("sfUser", "RKK666").option("sfPassword", "1+1Rahul").option("sfDatabase", "RKKDB").option("sfSchema", "RKKSCHEMA").option("sfRole", "ACCOUNTADMIN").option("sfWarehouse", "COMPUTE_WH").option("dbtable", "first").load()
    firstyearsales.show()

    firstyearsales.printSchema()

    val aggdf = firstyearsales.groupBy("MAKER").agg(sum("TOTAL").cast(IntegerType).as("total year sales"))

    aggdf.write.format("parquet").option("header", "true").option("multiline", "true").mode("overwrite").save("s3://projbuck2/total_sales_by_maker.parquet")


    firstyearsales.write.format("parquet").option("header", "true").option("multiline", "true").mode("overwrite").save("s3://processedproj/firstyearsales.parquet")

    val secondyearsales = Source.fromURL("https://mocki.io/v1/93e92677-daa3-4681-91be-8e2b44b85a6e").mkString

    val rdd = sc.parallelize(List(secondyearsales))

    val df = spark.read.option("multiline", "true").json(rdd).select("SN0", "MONTH", "YEAR", "Maker", "ELECTRIC(BOV)", "PETROL", "TOTAL")

    df.show()

    df.printSchema()

    df.write.format("parquet").option("header", "true").option("multiline", "true").mode("overwrite").save("s3://processedproj/secondyearsales.parquet")

    val thirdyearsales = Source.fromURL("https://mocki.io/v1/1f16b228-f5c5-41ae-ae84-2b1132ac3557").mkString


    val rdd1 = sc.parallelize(List(thirdyearsales))

    val df1 = spark.read.option("multiline", "true").json(rdd1).select("SN0", "MONTH", "YEAR", "Maker", "ELECTRIC(BOV)", "PETROL", "TOTAL")

    df1.show()

    df1.printSchema()

    df1.write.format("parquet").option("header", "true").option("multiline", "true").mode("overwrite").save("s3://processedproj/thirdyearsales.parquet")

    val fourthyearsales = Source.fromURL("https://mocki.io/v1/5c7667ea-eaa1-4406-bffa-a531d6e9e39c").mkString


    val rdd2 = sc.parallelize(List(fourthyearsales))

    val df2 = spark.read.option("multiline", "true").json(rdd2).select("SN0", "MONTH", "YEAR", "Maker", "ELECTRIC(BOV)", "PETROL", "TOTAL")

    df2.show()

    df2.printSchema()

    df2.write.format("parquet").option("header", "true").option("multiline", "true").mode("overwrite").save("s3://processedproj/fourthyearsales.parquet")


    val finalyearsales = spark.read.format("parquet").option("multiline", "true").option("header", "true").load("s3://salesdata11/sales2023.parquet").select("SN0", "MONTH", "YEAR", "Maker", "ELECTRIC(BOV)", "PETROL", "TOTAL")

    finalyearsales.show()

    finalyearsales.printSchema()

    val aggdf1 = finalyearsales.groupBy("MAKER").agg(sum("TOTAL").cast(IntegerType).as("total year sales"))

    aggdf1.write.format("parquet").option("header", "true").option("multiline", "true").mode("overwrite").save("s3://projbuck2/totalsalesbymaker4.parquet")


    finalyearsales.write.format("parquet").option("header", "true").option("multiline", "true").mode("overwrite").save("s3://processedproj/finalyearsales.parquet")



in all the aboe steps we will always be priting the schema to check the data is in a valid dataframe








- Step 3:Now all the year sales data has been written as parquet format in processed data AWS S3 storage system now we will retrive the data and convert it into a single dataframe and write it as parquet form in AWS S3 for further processing



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


    combinedDF.coalesce(1).write.format("net.snowflake.spark.snowflake").option("sfURL", "https://efvwdqe-wj90521.snowflakecomputing.com").option("sfAccount", "PJ12363").option("sfUser", "RKK666").option("sfPassword", "1+1Rahul").option("sfDatabase", "RKKDB").option("sfSchema", "RKKSCHEMA").option("sfRole", "ACCOUNTADMIN").option("sfWarehouse", "COMPUTE_WH").option("dbtable", "totalsales").mode("overwrite").save()





