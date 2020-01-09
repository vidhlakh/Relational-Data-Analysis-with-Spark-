from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import * 
import sys
spark = SparkSession.builder.master("local").appName("my-spark-app2").getOrCreate()
#create book rdd
lines=spark.sparkContext.textFile(sys.argv[1])  
parts=lines.map(lambda l:l.split("\t"))
bk_rdd=parts.map(lambda b: (b[0],b[1])) 
    
#create purchase rdd
lines_pur=spark.sparkContext.textFile(sys.argv[2])  
parts_pur=lines_pur.map(lambda l:l.split("\t"))
purchase_rdd=parts_pur.map(lambda p: (p[2],p[3],int(p[4])))   
purchase_rdd1=parts_pur.map(lambda p: (p[2],[p[3],int(p[4])]))  

#create book df
bookSchema="isbn name"  
bookfields = [StructField(field_name, StringType(), True) for field_name in bookSchema.split()] 
bk_schema=StructType(bookfields) 
bk_df=spark.createDataFrame(bk_rdd, bk_schema) 
#create purchase df
purchasefields = [StructField('isbn',StringType(),True),StructField('seller',StringType(),True), StructField('price', IntegerType(), True)] 
purchase_schema=StructType(purchasefields) 
purchase_df=spark.createDataFrame(purchase_rdd, purchase_schema) 

#purchase_rdd.join(bk_rdd)

purchase_df.createOrReplaceTempView("purchase_table")
bk_df.createOrReplaceTempView("bk_table")
bk_tab=spark.sql("select * from bk_table") 
purchase_bk_table=spark.sql("select purchase_table.isbn,bk_table.name,purchase_table.seller,purchase_table.price from purchase_table JOIN bk_table ON purchase_table.isbn=bk_table.isbn") 
#purchase_bk_table.show()
purchase_bk_table.createOrReplaceTempView("lw_table")  
lw=spark.sql("select name, price, seller from lw_table ")  
lw_final=spark.sql("select name,MIN(price) as price from lw_table group by name") 
lw_final.createOrReplaceTempView("lw_final_table")
llw=spark.sql("select name from lw_final_table where price in (select price from lw_table where seller='Amazon')")
print(llw.show())

#RDD version

#Join purchase and book rdd
purchase_bk_rdd=bk_rdd.join(purchase_rdd1)
#Get values of purchase rdd having book name, seller and price
purchase_bk_rdd_content=purchase_bk_rdd.map(lambda x: x[1])

# convert the list of values to key value pair
new_rdd=purchase_bk_rdd_content.mapValues(lambda x: (x[1],x[0]))
# Reduce by key book name
nw1=new_rdd.reduceByKey(min)  
#Filter values having seller as Amazon
nw2=nw1.filter(lambda x: "Amazon" in x[1])
# Display the book names
print(nw2.keys().collect())


 
