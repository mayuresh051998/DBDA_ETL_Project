#!/usr/bin/env python
# coding: utf-8

# Intialization
import os
import sys

os.environ["SPARK_HOME"] = "/home/talentum/spark"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
# In below two lines, use /usr/bin/python2.7 if you want to use Python 2
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3.6" 
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.7-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")

# NOTE: Whichever package you want mention here.
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.11:0.6.0 pyspark-shell' 
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.11:2.4.0 pyspark-shell'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.11:0.6.0,org.apache.spark:spark-avro_2.11:2.4.3 pyspark-shell'
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.11:0.6.0,org.apache.spark:spark-avro_2.11:2.4.0 pyspark-shell'


from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("Test Spark").getOrCreate()

# On yarn:
# spark = SparkSession.builder.appName("Spark SQL basic example").enableHiveSupport().master("yarn").getOrCreate()
# specify .master("yarn")

sc = spark.sparkContext


import pandas as pd
import numpy as np
import re
from pyspark.sql.functions import*
import pyspark.sql.functions as F
from pyspark.sql.types import *

df=pd.read_csv("file:///home/talentum/shared/project/kaggleLocaldata.csv")


print(df.columns)

df=df.drop(["Unnamed: 0.2", "Unnamed: 0.1", "Unnamed: 12" , "Unnamed: 13" , "Unnamed: 14" , "Unnamed: 15"], axis=1)

N = 1000
new_df = df.iloc[:N]

new_df1=new_df.copy()

from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="dbdaproject2022@gmail.com")


#### Function for serching exact location from given address
def extract_clean_address(address):
    try:
        location = geolocator.geocode(address)
        return location.address
    except:
        return ''

new_df1['cleanAddress'] = new_df1.apply(lambda x: extract_clean_address(x['Location']), axis =1 )

### Function to get latitude and longnitude from location.
    
def extract_lat_long(address):
    try:
        location = geolocator.geocode(address)
        return [location.latitude, location.longitude]
    except:
        return ''


# In[17]:


#generating latitude and lognitude values form clean address 
new_df1['lat_long'] = new_df1.apply(lambda x: extract_lat_long(x['cleanAddress']) , axis =1)
new_df1['latitude'] = new_df1.apply(lambda x: x['lat_long'][0] if x['lat_long'] != '' else '', axis =1)
new_df1['longitude'] = new_df1.apply(lambda x: x['lat_long'][1] if x['lat_long'] != '' else '', axis =1)
new_df1.drop(columns = ['lat_long'], inplace = True)


from geopy.point import Point
ct=[]
st=[]
cnt=[]
zp=[]
for lat,long in zip (new_df1['latitude'],new_df1['longitude']):

    location = geolocator.reverse(Point(lat,long))
    address = location.raw['address']
    
    # travese the data
    city = address.get('city', '')
    ct.append(city)
    
    state = address.get('state', '')
    st.append(state)
    
    country = address.get('country', '')
    cnt.append(country)
    
    zipcode = address.get('postcode')
    zp.append(zipcode)
new_df1['City']=ct
new_df1['State']=st
new_df1['Country']=cnt
new_df1['Zipcode']=zp


new_df1.head(5)

print(new_df1.columns)


Final_df=new_df1.drop(["Unnamed: 0", "UpVotes","DownVotes","Location","latitude","longitude"],axis=1)

Final_df.head(5)


type(Final_df)

Final_df

Final_df['AboutMe']

Final_df['AboutMe'] = Final_df['AboutMe'].replace("<[^>]*>", "",regex=True).replace("\n","",regex=True)

Final_df['AboutMe']

Final_df

type(Final_df)

Final_df=Final_df.astype(str)

final_df=Spark_df=spark.createDataFrame(Final_df)


##storing Clean data of Gmailattchfile in json fromat in hdfs # for storing it used g=hdfs:// in path 

final_df.write.json("hdfs:///user/talentum/projectCleanedFiles/KaggleDataclean")
