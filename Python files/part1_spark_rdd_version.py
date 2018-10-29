#!/usr/bin/env python
# coding: utf-8

# In[1]:


# importing the classes and functions
import pyspark
from pyspark import *
from pyspark.conf import *
from pyspark.sql import *

# In[2]:


# setting the data files for users and their friend list
userdata_path = 'userdata.txt'
friendsdata_path = 'soc_file.txt'
app_name = 'Userdata and their Friends'
master = 'local'

# In[3]:


# configuring the Spark and setting the master & app name
spark = SparkConf().setAppName(app_name).setMaster(master)
sc = SparkContext(conf=spark)

# In[4]:


# creating the RDD from the inputfile
friends_data = sc.textFile(friendsdata_path)


# checking the input data
# for line in friends_data.collect():
#    print(line)


# In[5]:


def mutual_friend_mapper(line):
    user_friends = line.split("\t")

    user = user_friends[0]
    friends = user_friends[1].split(",")

    friends_second_list = list(friends)
    mutual_friend_list = list()

    if friends:
        for friend in friends:
            if friend != '':
                friends_second_list.remove(friend)
                friends_first_list = list(friends_second_list)

                if int(friend) > int(user):
                    mutual_friend_list.append(((int(user), int(friend)), friends_first_list))
                    friends_second_list.append(friend)
                else:
                    mutual_friend_list.append(((int(friend), int(user)), friends_first_list))
                    friends_second_list.append(friend)

    _mutual_friend_list = list()

    for line in mutual_friend_list:
        temp = list(line)
        temp[0] = str(temp[0]).replace('(', '').replace(')', '')
        temp[1] = set(temp[1])
        line = list(temp)
        _mutual_friend_list.append(line)

    return _mutual_friend_list


# In[6]:


temp = mutual_friend_mapper('9	0,6085,18972,19269')

print(temp)

'''for line in temp:
    temp = list(line)
    temp[0] = str(temp[0]).replace('(','').replace(')','')
    temp[1] = set(temp[1])
    line = list(temp)
    print(line)'''

# In[7]:


paired_data_rdd = friends_data.flatMap(mutual_friend_mapper)

mutual_friends_rdd = paired_data_rdd.reduceByKey(lambda list1, list2: len(list1.intersection(list2))).filter(
    lambda x: x[1] != 0)

mutual_friends_rdd.saveAsTextFile('spark_rdd_version/mutual_friends.txt')

# In[8]:


top_ten_mutual_friends_rdd = mutual_friends_rdd.map(lambda y: (y[1], y[0])).sortByKey(ascending=False).collect()[0:10]

for line in top_ten_mutual_friends_rdd:
    print(line)

# In[9]:


userdata_rdd = sc.textFile('userdata.txt')

friends_details_rdd = userdata_rdd.map(lambda x: x.split(",")).map(lambda x: (int(x[0]), x[1:]))

for line in friends_details_rdd.collect():
    print(line)

# In[10]:


result = list()

for x in top_ten_mutual_friends_rdd:
    friend_count = x[0]
    friend1 = int(x[1].split(', ')[0])
    friend2 = int(x[1].split(', ')[1])

    friend1_data = friends_details_rdd.lookup(friend1)[0]
    friend2_data = friends_details_rdd.lookup(friend2)[0]

    line = str(friend_count) + '\t' + friend1_data[0] + '\t' + friend1_data[1] + '\t' + friend1_data[2] + '\t' + \
           friend2_data[0] + '\t' + friend2_data[1] + '\t' + friend2_data[2]

    # print(line)
    result.append(line)

sc.parallelize(result).saveAsTextFile('spark_rdd_version/top_ten_mutual_friends.txt')

# In[ ]:
