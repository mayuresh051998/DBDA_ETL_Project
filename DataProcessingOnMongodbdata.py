#!/usr/bin/env python
# coding: utf-8
pip install geopy

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

#Entrypoint 2.x
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark SQL basic example").enableHiveSupport().getOrCreate()

# On yarn:
# spark = SparkSession.builder.appName("Spark SQL basic example").enableHiveSupport().master("yarn").getOrCreate()
# specify .master("yarn")

sc = spark.sparkContext

import pandas as pd
import numpy as np
import re
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

New_df=spark.read.csv("file:///home/talentum/shared/project/MongoDBdata1.csv",header=True,inferSchema=True)

New_df.printSchema()

##AboutMe Column clening code.
def cleanText(df):
    df = df.withColumn('AboutMe', regexp_replace('AboutMe', '<[^>]*>', ''))
    df = df.withColumn('AboutMe', regexp_replace('AboutMe', '\n', '.'))
    return df  

#### Address cleaning
from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="dbdaproject2022@gmail.com")

#### Function for serching exact location from given address
def extract_clean_address(address):
    try:
        location = geolocator.geocode(address)
        return location.address
    except:
        return ''
### Function to get latitude and longnitude from location.
    
def extract_lat_long(address):
    try:
        location = geolocator.geocode(address)
        return [location.latitude, location.longitude]
    except:
        return ''

New_df = New_df.toPandas()
type(New_df)

New_df['cleanAddress'] = New_df.apply(lambda x: extract_clean_address(x['Location']), axis =1 )
New_df.head(5)

#generating latitude and lognitude values form clean address 
New_df['lat_long'] = New_df.apply(lambda x: extract_lat_long(x['cleanAddress']) , axis =1)
New_df['latitude'] = New_df.apply(lambda x: x['lat_long'][0] if x['lat_long'] != '' else '', axis =1)
New_df['longitude'] = New_df.apply(lambda x: x['lat_long'][1] if x['lat_long'] != '' else '', axis =1)
New_df.drop(columns = ['lat_long'], inplace = True)

New_df.head(2)

from geopy.point import Point
ct=[]
st=[]
cnt=[]
zp=[]
for lat,long in zip (New_df['latitude'],New_df['longitude']):

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
New_df['City']=ct
New_df['State']=st
New_df['Country']=cnt
New_df['Zipcode']=zp

New_df.head(5)

Final_df=New_df.drop(["_c0","UpVotes","DownVotes","Location","latitude","longitude"],axis=1)

type(Final_df)

Final_df.head(5)

Spark_df=spark.createDataFrame(Final_df)

Spark_df.show(10)


### AboutME Cleaning
Spark_df=cleanText(Spark_df)
Spark_df.count()


#dropping null values form DataFrame
final_df=Spark_df.dropna(subset=["cleanAddress","Zipcode"])

final_df.count()

final_df.select("AccountId","Reputation","CreationDate","DisplayName","WebsiteUrl","Contact Num").show()

final_df.select("AboutMe","cleanAddress","City","State","Country","Zipcode").show()

##storing Clean data of mongodbfile in json fromat in hdfs # for storing it used g=hdfs:// in path 

final_df.write.json("hdfs:///user/talentum/projectCleanedFiles/MongoDBClean")