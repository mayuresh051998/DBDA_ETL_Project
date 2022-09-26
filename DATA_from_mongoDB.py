#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# In[1]:


pip install pymongo


# In[1]:


import pymongo
import pandas as pd


# In[2]:


client=pymongo.MongoClient("mongodb://127.0.0.1:27017")


# In[3]:


db= client["admin"]


# In[4]:


print(db)


# In[5]:


mycollection=db["project"]


# In[6]:


print(mycollection)


# In[7]:


one_record=mycollection.find_one()


# In[8]:


print(one_record)


# In[9]:


all_records=mycollection.find()


# In[10]:


#print(all_records)


# In[11]:


##converting dictionary to dataframe
list_cursor=list(all_records)


# In[12]:


df=pd.DataFrame(list_cursor)


# In[13]:


df.head(2)


# In[14]:


df.shape


# In[16]:


df=df.drop('_id',axis=1)


# In[17]:


#Saving dataFrame on local machine

df.to_csv(r'/home/talentum/shared/project/MongoDBdata1.csv', index=True,header=True)


# In[ ]:




