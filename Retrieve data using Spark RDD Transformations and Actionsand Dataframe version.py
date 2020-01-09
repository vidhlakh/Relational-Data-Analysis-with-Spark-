from operator import add

import sys
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
sparkSession = SparkSession.builder.master("local").appName("my-spark-app").getOrCreate()

customers=sparkSession.sparkContext.textFile(sys.argv[1])
books=sparkSession.sparkContext.textFile(sys.argv[2])
purchases = sparkSession.sparkContext.textFile(sys.argv[3]) 


#First Query
sel_price=purchases.map(lambda x: (x.split('\t')[3],int(x.split('\t')[4]))) 

gr_sel_price=sel_price.reduceByKey(add)   
print("Seller and Price",gr_sel_price.collect()) 

#Second Query
new_cust=customers.map(lambda x: (x.split('\t')[0],x.split('\t')[1])).mapValues(lambda x: x.split(" ")[1])  

cid_price=purchases.map(lambda x: (x.split('\t')[1],int(x.split('\t')[4]))) 

cust_price=new_cust.join(cid_price)  


mp=cust_price.map(lambda x: x[1]) 
print("Family who spent maximum money on books",mp.reduceByKey(add).max())

