# Databricks notebook source
# MAGIC %md
# MAGIC Importing Packages

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import  StringType,StructType,StructField,IntegerType,ArrayType
from pyspark.sql.functions import col,udf,explode,split,array,substring,length,expr,to_date
import xml.etree.ElementTree as ET

# COMMAND ----------

# MAGIC %md
# MAGIC Loading customer and individual tables from SQL database

# COMMAND ----------

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
user = "sql-username"
password = "sql-password"
url="jdbc-connection-url"

individual_df = (spark.read.format("jdbc") \
                 .option("driver", driver) \
                 .option("url", url)  \
                 .option("dbtable", "Individual") \
                 .option("user", user) \
                 .option("password", password) \
                 .load())

customer_df = (spark.read.format("jdbc").option("driver", driver).option("url", url).option("dbtable", "customer").option("user", user).option("password", password).load())


# COMMAND ----------

#selecting required column from individual_df 

individual_df=individual_df.select(col("CustomerID").alias("individualID"),"ContactID","Demographics")


# COMMAND ----------

#performing join on both customer_df and individual_df dataframes and selecting required columns

cust_ind_df=customer_df.join(individual_df,customer_df.CustomerID == individual_df.individualID)
cust_ind_df=cust_ind_df.select("CustomerID","TerritoryID","AccountNumber","CustomerType","ContactID","Demographics","ModifiedDate")


# COMMAND ----------

# MAGIC %md
# MAGIC Creating UDF to extract value from demographics column

# COMMAND ----------

@udf
def extract_ab(xml):
    doc = ET.fromstring(xml)
    l = len(doc)
    ids = []
    for r in range(l):
        ids.append(doc[r].text)
    return ids


# COMMAND ----------

#using above udf to extract the values into string
cust_ind_df = cust_ind_df.withColumn('XMLColumn', extract_ab(cust_ind_df['Demographics']))

#removing first and last character from the extracted value
cust_ind_df=cust_ind_df.withColumn("XMLColumn", expr("substring(XMLColumn, 2, length(XMLColumn)-2)"))

# COMMAND ----------

#adding the required columns and adding the value by spliting the xml column and selecting the required columns

lst=['TotalPurchaseYTD','DateFirstPurchase','BirthDate','MaritalStatus','YearlyIncome','Gender','TotalChildren','NumberChildrenAtHome','Education','Occupation','HomeOwnerFlag','NumberCarsOwned','CommuteDistance']
for i in range(len(lst)):
    cust_ind_df=cust_ind_df.withColumn(lst[i],split(col("XMLColumn"),",").getItem(i) )
    
cust_ind_df=cust_ind_df.select("CustomerID","ContactID","ModifiedDate",'TotalPurchaseYTD','DateFirstPurchase','BirthDate','MaritalStatus','YearlyIncome','Gender','TotalChildren','NumberChildrenAtHome','Education','Occupation','HomeOwnerFlag','NumberCarsOwned','CommuteDistance')


# COMMAND ----------

#modifying the datatypes

cust_ind_df=cust_ind_df.withColumn("BirthDate", to_date(expr("substring(BirthDate, 2, length(BirthDate)-2)"), "yyyy-MM-dd"))
cust_ind_df=cust_ind_df.withColumn("DateFirstPurchase", to_date(expr("substring(DateFirstPurchase, 2, length(DateFirstPurchase)-2)"), "yyyy-MM-dd"))

cust_ind_df=cust_ind_df.select("CustomerID","ContactID",col('TotalPurchaseYTD').cast('int').alias('TotalPurchaseYTD'),'DateFirstPurchase','BirthDate','MaritalStatus','YearlyIncome','Gender',col('TotalChildren').cast('int').alias('TotalChildren'),col('NumberChildrenAtHome').cast('int').alias('NumberChildrenAtHome'),'Education','Occupation',col('HomeOwnerFlag').cast('int').alias('HomeOwnerFlag'),col('NumberCarsOwned').cast('int').alias('NumberCarsOwned'),'CommuteDistance',"ModifiedDate")



# COMMAND ----------

# MAGIC %md
# MAGIC Writing the data into sql table

# COMMAND ----------

cust_ind_df.write.format("jdbc") \
  .mode("overwrite") \
  .option("url", url) \
  .option("dbtable", "dbo.cust_ind") \
  .option("user", user) \
  .option("password", password) \
  .save()
