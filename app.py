from pyspark.sql import SparkSession  
from pyspark.sql.functions import *  
from pyspark.sql.window import Window  
  
spark=SparkSession.builder.appName("SCD Type 2").getOrCreate()  
  
#read intial data  
 df=spark.read.format("CSV").option("header","true").option("inferschema","true").loa
 d("D:/cust_ini.csv")  
df_ini=df.withColumn("start_date",date_add(current_date(),-13)).\  
 withColumn("end_date",lit('')).\  
 withColumn("current_flag",lit('Y'))  
df_ini.show()  
