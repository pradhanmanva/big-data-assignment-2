#!/usr/bin/env python
# coding: utf-8

# In[1]:


# setup
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

# In[2]:


# setting file paths
user_path = 'userdata.txt'
friends_path = 'soc_file.txt'

# create Spark context with Spark configuration
conf = SparkConf().setAppName("Userdata and Social File.")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# In[3]:


# preprocessing the userdata

# read input user text file to RDD
users = sc.textFile(user_path)
user_data = list()

# collect the RDD to a list
llist = users.collect()

# checking the line type 
for line in llist:
    x = line.split(',')

    user = int(x[0])
    details = list()
    details.append(user)

    for i in x[1:]:
        details.append(i)

    user_data.append(tuple(details))

# checking for the records as tuples
# for line in user_data:
#    print(line)

# setting up the file headers
user_columns = ['userid', 'firstname', 'lastname', 'address', 'city', 'state', 'zipcode', 'country', 'username', 'dob']

# converting the RDD to the DataFrame
user_df = sqlContext.createDataFrame(user_data, user_columns)

# printing the DF
user_df.show(5)
user_df.printSchema()
user_df.count()

# In[4]:


# preprocessing the social friends' data

# read input social friends text file to RDD
friends = sc.textFile(friends_path)
friends_data = list()

# collect the RDD to a list
llist = friends.collect()

# checking the line type 
for line in llist:
    x = line.split('\t')
    user = int(x[0])
    friend_list = list()

    for i in x[1].split(','):
        if i is not '':
            friend_list.append(int(i))

    friends_data.append(tuple([user, friend_list]))

# checking for the records as tuples
# for line in friends_data:
#   print(line)

# setting file headers
friends_columns = ['userid', 'friend_list']

# creating friends df
friends_df = sqlContext.createDataFrame(friends_data, friends_columns)

# printing the DF
friends_df.show(5)
friends_df.printSchema()
friends_df.count()

# In[5]:


# crossproduct of the friends database and removing duplicate values and redundant values
cross_join = friends_df.withColumnRenamed('userid', 'id1').withColumnRenamed('friend_list', 'friend1').crossJoin(
    friends_df.withColumnRenamed('userid', 'id2').withColumnRenamed('friend_list', 'friend2')).where(
    'id1 != id2 and id1<id2').withColumn('check_friendship', lit(0).cast(IntegerType()))

cross_join.show(5)
cross_join.printSchema()


# cross_join.count()


# In[6]:


# filtering the mutual friends list
def checking_friendship(user1, user1_friends, user2, user2_friends):
    if user1 in user2_friends and user2 in user1_friends:
        return 1
    return 0


print(checking_friendship(1, [3, 5, 7, 9], 2, [6, 8]))
print(checking_friendship(1, [3, 4, 5, 7, 9], 4, [1, 6, 8, 10]))


# In[7]:


# intersection of the mutual friend list
def mutual_friends(list1, list2):
    set_friends1 = set(list1)
    set_friends2 = set(list2)
    return len(set_friends1.intersection(set_friends2))


# checking how the function works
print(mutual_friends([1, 2, 3, 4, 4], [3, 4, 5, 6, 6]))

# In[9]:


# checking if both users are friends
checking_friendship_function = udf(lambda a, b, c, d: checking_friendship(a, b, c, d), IntegerType())

# derieving the mutual friends' numbers and removing the null values.
mutual_friends_function = udf(lambda x, y: mutual_friends(x, y), IntegerType())

cross_join_df = cross_join.withColumn('check_friendship',
                                      checking_friendship_function(col('id1'), col('friend1'), col('id2'),
                                                                   col('friend2'))).where(
    "check_friendship == 1").withColumn('check_friendship',
                                        mutual_friends_function(col('friend1'), col('friend2'))).where(
    'check_friendship != 0')

cross_join_df.show()
cross_join_df.printSchema()
# print(cross_join_df.count())


# In[22]:


# putting the number of the mutual friends for user1 and user2
_cross_join_df = cross_join_df.drop('friend1').drop('friend2')

_cross_join_df.show()


# In[11]:


# printing the format as desired in the output file.
def print_format(x, y, z):
    return str(x) + ', ' + str(y) + '\t' + str(z)


# checking how the function works
print(print_format('1', '2', '3'))

# In[24]:


# Defining the UDF and then applying it to the columns for desired output
print_format_function = udf(lambda x, y, z: print_format(x, y, z), StringType())

_cross_join_df_ = _cross_join_df.withColumn('check_friendship', print_format_function(col('id1'), col('id2'),
                                                                                      col('check_friendship'))).drop(
    col('id1')).drop(col('id2'))

_cross_join_df_.show(5)

# saving the question 1 answer into a text file
_cross_join_df_.write.text('spark_sql_version/mutual_friend_number.txt')

# In[25]:


# ordering in the descending order and then printing the top 10 for
sorted_data = _cross_join_df.orderBy(col('check_friendship').desc()).take(10)

# In[26]:


# printing each line to check the file
for line in sorted_data:
    print(line)

# printing the cross join df schema
_cross_join_df.printSchema()

# printing the column names
print(_cross_join_df.columns)

# In[27]:


# creating the dataframe for the sorted data
sorted_data_df = sqlContext.createDataFrame(sorted_data, _cross_join_df.columns)

# looking at the sorted data df
sorted_data_df.show()

# In[28]:


# joining the tables using inner_join
inner_join = user_df.join(friends_df, user_df.userid == friends_df.userid).drop(friends_df.userid).drop('city', 'state',
                                                                                                        'zipcode',
                                                                                                        'country',
                                                                                                        'username',
                                                                                                        'dob',
                                                                                                        'friend_list')

inner_join.show()

# In[31]:


# creating aliases for the new formed tables
users = inner_join.alias('users')
mutual_friends_table = sorted_data_df.alias('mutual_friends_table')

# creating a new df using userdata and social friends and final_top_ten_mutual_friends_userdata
tab1 = mutual_friends_table.join(users, (users.userid == mutual_friends_table.id1) | (
            users.userid == mutual_friends_table.id2)).drop(users.userid).groupBy(mutual_friends_table.id1,
                                                                                  mutual_friends_table.id2,
                                                                                  mutual_friends_table.check_friendship).agg(
    collect_list(users.firstname).alias('f'), collect_list(users.lastname).alias('n'),
    collect_list(users.address).alias('a')).orderBy(col('check_friendship').desc())
tab1.show()

tab2 = tab1.select(mutual_friends_table.check_friendship, tab1.f[0].alias('A_firstname'), tab1.n[0].alias('A_lastname'),
                   tab1.a[0].alias('A_address'), tab1.f[1].alias('B_firstname'), tab1.n[1].alias('B_lastname'),
                   tab1.a[1].alias('B_address'))
tab2.show()


# In[32]:


def _print_format(k, x, y, z, a, b, c):
    return str(k) + '\t' + str(x) + '\t' + str(y) + '\t' + str(z) + '\t' + str(a) + '\t' + str(b) + '\t' + str(c)


print(_print_format('1', '2', '3', '4', '5', '6', '7'))

# In[33]:


_print_format_function = udf(lambda k, x, y, z, a, b, c: _print_format(k, x, y, z, a, b, c), StringType())

_tab2_df = tab2.withColumn('check_friendship',
                           _print_format_function(col('check_friendship'), col('A_firstname'), col('A_lastname'),
                                                  col('A_address'), col('B_firstname'), col('B_lastname'),
                                                  col('B_address'))).drop('A_firstname').drop('A_lastname').drop(
    'A_address').drop('B_firstname').drop('B_lastname').drop('B_address')

_tab2_df.write.text('spark_sql_version/top_ten_users_mutual_friends.txt')

# In[ ]:
