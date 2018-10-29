#!/usr/bin/env python
# coding: utf-8

# In[1]:


# setup
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

# In[2]:


# setting paths for data files
business_path = 'business.csv'
review_path = 'review.csv'

# create Spark context with Spark configuration
conf = SparkConf().setAppName("Business and Reviews File.")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# In[3]:


# preprocessing the business data

# read input business csv file to RDD
business = sc.textFile(business_path)
business_data = list()

# collect the RDD to a list
llist = business.collect()

# checking the line type 
for line in llist:
    business_data.append(tuple(line.split('::')))

# checking for the records as tuples
# for line in business_data:
#    print(line)

# setting up the file headers
business_columns = ['businessid', 'address', 'categories']

# converting the RDD to the DataFrame
business_df = sqlContext.createDataFrame(business_data, business_columns)

# printing the DF
business_df.show()
print(business_df.count())

# In[4]:


# preprocessing the review data

# read input review csv file to RDD
review = sc.textFile(review_path)
review_data = list()

# collect the RDD to a list
llist = review.collect()

# checking the line type 
for line in llist:
    review_data.append(tuple(line.split('::')))

# checking for the records as tuples
# for line in review_data:
#    print(line)

# setting up the file headers
review_columns = ['reviewid', 'userid', 'businessid', 'stars']

# converting the RDD to the DataFrame
review_df = sqlContext.createDataFrame(review_data, review_columns)

# printing the DF
review_df = review_df.drop(review_df.reviewid)
review_df.show()
print(review_df.count())

# In[5]:


# filtering Stanford businesses
temp = business_df.filter(business_df.address.like('%Stanford, CA%')).drop(business_df.address).drop(
    business_df.categories)

temp.show()

print(temp.count())

# In[6]:


# table joins for business and table
business_review_table = temp.join(review_df, review_df.businessid == temp.businessid).distinct().drop(
    temp.businessid).drop(review_df.businessid)

business_review_table.show()

print(business_review_table.count())


# In[7]:


def print_format(x, y):
    return str(x) + '\t' + str(y)


print(print_format('1', '2'))

# In[8]:


# saving the question 1 answer into a text file
print_format_function = udf(lambda x, y: print_format(x, y), StringType())

_business_review_rating_df = business_review_table.withColumn('stars',
                                                              print_format_function(col('userid'), col('stars'))).drop(
    col('userid'))

_business_review_rating_df.write.text('spark_sql_version/userid_rating_table.txt')


# In[9]:


def to_float(x):
    return float(x)


print(to_float('1.0'))

# In[10]:


to_float_function = udf(lambda x: to_float(x), FloatType())

review_df = review_df.withColumn('stars', to_float_function(col('stars')))

avg_review_df = review_df.groupBy('businessid').avg('stars')

avg_review_df.show()

# In[11]:


top_ten_business = avg_review_df.orderBy(col('avg(stars)').desc()).take(10)

top_ten_business_df = sqlContext.createDataFrame(top_ten_business, avg_review_df.columns).withColumnRenamed(
    'businessid', 'bid')

top_ten_business_df.printSchema()

# In[12]:


business_rating_table = business_df.join(top_ten_business_df,
                                         (business_df.businessid == top_ten_business_df.bid)).distinct().drop('bid')

business_rating_table.show()


# In[13]:


def print_format(x, y, a, b):
    return str(x) + '\t' + str(y) + '\t' + str(a) + '\t' + str(b)


print(print_format('1', '2', '3', '4'))

# In[14]:


print_format_function = udf(lambda x, y, a, b: print_format(x, y, a, b), StringType())

_business_rating_table = business_rating_table.withColumn('businessid',
                                                          print_format_function(col('businessid'), col('address'),
                                                                                col('categories'),
                                                                                col('avg(stars)'))).drop(
    'address').drop('categories').drop('avg(stars)')

_business_rating_table.write.text('spark_sql_version/business_rating_table.txt')

# In[ ]:
