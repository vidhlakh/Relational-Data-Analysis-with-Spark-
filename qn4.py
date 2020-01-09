import sys
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import * 

spark = SparkSession.builder.master("local").appName("my-spark-app2").getOrCreate()

#create RDD for customer data
lines=spark.sparkContext.textFile(sys.argv[1])  
parts=lines.map(lambda l:l.split("\t"))
cust_rdd=parts.map(lambda b: (b[0],b[1]))
#set schema for customer data
custfields = [StructField('cid',StringType(),True),StructField('name',StringType(),True)] 
cust_schema=StructType(custfields) 
#create customer dataframe which has cid and name columns
cust_df=spark.createDataFrame(cust_rdd, cust_schema)  

#create RDD for purchase data
lines=spark.sparkContext.textFile(sys.argv[2])  
parts=lines.map(lambda l:l.split("\t"))
pur_rdd=parts.map(lambda p: (p[1],p[2]))

#set schema for purchase data
purfields = [StructField('cid',StringType(),True),StructField('isbn',StringType(),True)] 
pur_schema=StructType(purfields) 
#create customer dataframe which has cid and name columns
pur_df=spark.createDataFrame(pur_rdd, pur_schema) 

#Join 2 dataframes with cid
pur_cust=cust_df.join(pur_df, "cid") 

# Filter books bought by Harry Smith
harry_books=pur_cust.select("isbn").filter(pur_cust["name"]=="Harry Smith")
# Store books of Harry Smith in a list 
isbn_list = [row['isbn'] for row in harry_books.collect()]
# Select name of person by filtering books read by Harry
persons=pur_cust.select("name").filter(pur_cust.isbn.isin(isbn_list)).distinct()
persons.filter(persons["name"]!="Harry Smith").show()

#RDD version
#Join customer and purchase RDD     
cust_pur_rdd = cust_rdd.join(pur_rdd)      
cust_pur_rdd_values=cust_pur_rdd.map(lambda x: x[1]) 
#Filter books bought by Harry Smith
harry=cust_pur_rdd_values.filter(lambda x: "Harry Smith" in x)
harry_books=harry.map(lambda x:x[1]).collect()  
#Select persons by filtering the persons having books same as Harry
person_book=cust_pur_rdd_values.filter(lambda x: x[1] in harry_books)
persons=person_book.map(lambda x:x[0]).distinct()
print("Persons who bought all books that Harry bought") 
print(persons.filter(lambda x: "Harry Smith" not in x).collect())

