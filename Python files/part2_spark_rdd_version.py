#!/usr/bin/env python
# coding: utf-8

# In[1]:


# importing the classes and functions
import pyspark
from pyspark import *
from pyspark.conf import *
from pyspark.sql import *

# In[2]:


# setting paths for data files
business_path = 'business.csv'
review_path = 'review.csv'

# In[3]:


app_name = 'Business and Reviews Data'
master = 'local'

# In[4]:


# configuring the Spark and setting the master & app name
spark = SparkConf().setAppName(app_name).setMaster(master)
sc = SparkContext(conf=spark)

# In[5]:


# business data

# creating the RDD from the inputfile
business_data = sc.textFile(business_path).distinct().map(lambda x: x.split('::')).map(lambda x: (x[0], tuple(x[1:])))

# checking the input data
for line in business_data.collect():
    print(line)

# In[6]:


# reviews data

# creating the RDD from the inputfile
reviews_data = sc.textFile(review_path).distinct().map(lambda x: x.split('::')).map(
    lambda x: (str(x[2]), (str(x[1]), float(x[3]))))

# checking the input data
for line in reviews_data.collect():
    print(line)

# In[7]:


filtered_data = business_data.filter(lambda x: 'Stanford, CA' in x[1][0])

for line in filtered_data.collect():
    print(line)

print(len(filtered_data.collect()))

# In[10]:


# just showing the users and their ratings of the businesses in the 'Standford, CA'
business_review_table = filtered_data.join(reviews_data).map(lambda x: x[1][1]).distinct()

for line in business_review_table.collect():
    print(line)

# In[12]:


result = business_review_table.collect()
sc.parallelize(result).saveAsTextFile('spark_rdd_version/users_rating_Stanford.txt')


# In[44]:


def avg(x):
    temp = list()
    even = 0
    for i in x:
        for j in i:
            even = even + 1
            if even % 2 == 0:
                temp.append(j)

    return sum(temp) / len(temp)


temp = reviews_data.groupByKey().mapValues(avg).sortByKey(ascending=False).collect()[0:10]

# i=0
for line in temp:
    #     i=i+1
    #     if i<5:
    print(line)

# In[46]:


top_ten_avg_users_rating = sc.parallelize(temp)

for line in top_ten_avg_users_rating.collect():
    print(line)

# In[50]:


top_ten_business = top_ten_avg_users_rating.join(business_data).map(lambda x: (x[0], x[1][1][0], x[1][1][1], x[1][0]))

for line in top_ten_business.collect():
    print(line)

top_ten_business.saveAsTextFile('spark_rdd_version/top_ten_business.txt')

# In[ ]:
