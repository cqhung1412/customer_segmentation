# Databricks notebook source
!pip install -U dataprep numpy pandas markupsafe==2.0.1

# COMMAND ----------

import numpy as np
import pandas as pd
import math
from datetime import timedelta, datetime
from dataprep.clean import clean_country
import matplotlib.pyplot as plt
%matplotlib inline
plt.style.use("seaborn")
plt.rcParams["figure.figsize"] = (20, 5)

# COMMAND ----------

# df = pd.read_csv('../data/sales_asia.csv', sep=';', decimal=',')
# df.to_csv('../data/formatted_sales_asia.csv', sep=';', decimal='.')

# COMMAND ----------

df1 = spark.sql("SELECT * FROM `dev_ds_catalog`.`hungcq`.`cust_segm_sales_asia`")
df1.head()

# COMMAND ----------

df1_db = df1
df1 = df1.toPandas()

# COMMAND ----------

df1.head()

# COMMAND ----------

df1.tail()

# COMMAND ----------

df1.shape

# COMMAND ----------

df1.info()

# COMMAND ----------

# Splitting 'week.year' column on '.' and creating 'week' and 'year' columns
df1['week'] = df1['week.year'].astype(str).str.split('.').str[0]
df1['year'] = df1['week.year'].astype(str).str.split('.').str[1]
df1.head()

# COMMAND ----------

# Converting year and week into date, using Monday as first day of the week
df1['date'] = pd.to_datetime(df1['year'].map(str) + df1['week'].map(str) + '-1', format='%Y%W-%w')
df1.head()

# COMMAND ----------

df1.columns

# COMMAND ----------

# Removing unnecesary columns
df2 = df1.drop(['week.year', 'week', 'year'], axis=1)
df2.head()

# COMMAND ----------

#Rename columns
df2.rename({'revenue': 'monetary'}, axis="columns", inplace=True)
df2.head()

# COMMAND ----------

df2.info()

# COMMAND ----------

df2.describe()

# COMMAND ----------

# For starting dataset,
# We have 235574 transactions in the period of time icluded in the dataset
# Biggest transaction was 150,000 units. But it seems there was a reeturn of that amount as well, -150,000 units
# Most expensive purchase was 2.41 Millions

# COMMAND ----------

df2.isnull().sum()

# COMMAND ----------

# Let's view the period of time included in the dataset
df2['date'].min()

# COMMAND ----------

df2['date'].max()

# COMMAND ----------

# For starting dataset, initial max date is 2020-11-30

# COMMAND ----------

# Let's explore in how many different countries we have sales in that period
df2['country'].unique()

# COMMAND ----------

df2['country'].nunique()

# COMMAND ----------

# Transforming country codes into full country names with clean_country function from dataprep library
clean_country(df2, "country")['country_clean'].unique()

# COMMAND ----------

# Total number of customers in all countries
df2['id'].nunique()

# COMMAND ----------

# Putting date into the index for plotting the time series
df2b = df2.set_index("date")
df2b.head()

# COMMAND ----------

plt.style.use('ggplot')
plt.title('Units sold per week')
plt.ylabel('units')
plt.xlabel('date')
df2b['units'].plot(figsize=(20,5), c='dodgerblue')

# COMMAND ----------

plt.style.use('ggplot')
plt.title('Revenue per week')
plt.ylabel('units')
plt.xlabel('date')
df2b['monetary'].plot(figsize=(20,5), c='dodgerblue')

# COMMAND ----------

# For greater visibility in the plots we convert the dates to monthly periods and we aggregate the units and revenue of the same period
df2c = df2b.to_period("M")

# COMMAND ----------

df2c.head()

# COMMAND ----------

plt.style.use('ggplot')
df2c['units'].groupby('date').agg(sum).plot(figsize=(20,5), c='dodgerblue')
plt.title('Units sold per month')
plt.ylabel('units')
plt.xlabel('date')

# COMMAND ----------

plt.style.use('ggplot')
df2c['monetary'].groupby('date').agg(sum).plot(figsize=(20,5), c='dodgerblue')
plt.title('Revenue per month')
plt.ylabel('revenue')
plt.xlabel('date')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform data to obtain RFM

# COMMAND ----------

print('Sales from {} to {}'.format(df2['date'].min(),
                                    df2['date'].max()))

# COMMAND ----------

#Let's focus on sales from last 365 days since most recent date
period = 365
date_N_days_ago = df2['date'].max() - timedelta(days=period)

# COMMAND ----------

# We remove the rows with dates older than 365 days ago
df2 = df2[df2['date']> date_N_days_ago]

# COMMAND ----------

df2.reset_index(drop=True, inplace=True)

# COMMAND ----------

# df2.reset_index(drop=True, inplace=True)

# COMMAND ----------

df2.head()

# COMMAND ----------

df2.info()

# COMMAND ----------

# There are customers with the same 'id' in several countries. This causes errors in the monetary values
# Let's create a unique 'id+' identifier that combines country code and customer id
df3 = df2.copy()

df3['id+'] = df3['country'].map(str) + df3['id'].map(str)
df3.head()

# COMMAND ----------

# We set the NOW date one day after the last sale
NOW = df3['date'].max() + timedelta(days=1)
NOW

# COMMAND ----------

# We add a column, 'days_since_last_purchase', with the days between purchase date and the latest date
df3['days_since_purchase'] = df3['date'].apply(lambda x:(NOW - x).days)
df3.head()

# COMMAND ----------

df3[df3['id+']=='KR706854']

# COMMAND ----------

# Recency will be the minimum of 'days_since_last_purchase' for each customer
# Frequency will be the total number of orders in the period for each customer
aggr = {
    'days_since_purchase': lambda x:x.min(),
    'date': lambda x: len([d for d in x if d >= NOW - timedelta(days=period)])
}

# COMMAND ----------

rfm = df3.groupby(['id', 'id+', 'country']).agg(aggr).reset_index()
rfm.rename(columns={'days_since_purchase': 'recency',
                   'date': 'frequency'},
          inplace=True)

# COMMAND ----------

rfm

# COMMAND ----------

# We check customers with id 3790218 have different recency and frequency values per country
rfm[rfm['id']==3790218]

# COMMAND ----------

# We get the revenue of the last 365 days per customer
df3[df3['date'] >= NOW - timedelta(days=period)]\
    .groupby('id+')['monetary'].sum()

# COMMAND ----------

# Example: getting only the monetary value for specific customer with id 3790218
df3[ (df3['id'] == 3790218) & (df3['date'] >= NOW - timedelta(days=period))]\
    .groupby('id+')['monetary'].sum()

# COMMAND ----------

# We add the revenue from df3 of last period per customer to rfm dataframe
rfm['monetary'] = rfm['id+']\
    .apply(lambda x: df3[ (df3['id+'] == x) & (df3['date'] >= NOW - timedelta(days=period))]\
    .groupby(['id', 'country']).sum().iloc[0,0])
rfm.head()

# COMMAND ----------

# Checking monetary value is correct by checking on our biggest customer
rfm[rfm['monetary']==rfm['monetary'].max()]

# COMMAND ----------

rfm[rfm['frequency']==rfm['frequency'].max()]

# COMMAND ----------

# We check that customers with id 3790218 get a different monetary value per country
rfm[rfm['id']==3790218]

# COMMAND ----------

# Let's drop the column 'id+'
rfm.drop(['id+'], axis=1, inplace=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate the R, F and M scores

# COMMAND ----------

# We assign a rate between 1 and 5 depending on recency, monetary and frequency parameters
# We use the quintiles method, dividing every feature on groups that contain 20 % of the samples
quintiles = rfm[['recency', 'frequency', 'monetary']].quantile([.2, .4, .6, .8]).to_dict()
quintiles

# COMMAND ----------

# Assigning scores from 1 to 5
# Higher values are better for frequency and monetary, while lower values are better for recency
def r_score(x):
    if x <= quintiles['recency'][.2]:
        return 5
    elif x <= quintiles['recency'][.4]:
        return 4
    elif x <= quintiles['recency'][.6]:
        return 3
    elif x <= quintiles['recency'][.8]:
        return 2
    else:
        return 1

# COMMAND ----------

def fm_score(x, c):
    if x <= quintiles[c][.2]:
        return 1
    elif x <= quintiles[c][.4]:
        return 2
    elif x <= quintiles[c][.6]:
        return 3
    elif x <= quintiles[c][.8]:
        return 4
    else:
        return 5

# COMMAND ----------

# We asssign R, F and M scores to each customer
rfm['r'] = rfm['recency'].apply(lambda x: r_score(x))
rfm['f'] = rfm['frequency'].apply(lambda x: fm_score(x, 'frequency'))
rfm['m'] = rfm['monetary'].apply(lambda x: fm_score(x, 'monetary'))

# COMMAND ----------

rfm.head()

# COMMAND ----------

# Combine R, F and M scores to create a unique RFM score
rfm['rfm_score'] = rfm['r'].map(str) + rfm['f'].map(str) + rfm['m'].map(str)
rfm.head()

# COMMAND ----------

# With this rfm scores we would have 125 segments of customers
# To make a more simple segment map of 11 segments, we combine f and m scores, rounding them down
# fm = (f+m)/2

# COMMAND ----------

def truncate(x):
    return math.trunc(x)

# COMMAND ----------

rfm['fm'] = ((rfm['f'] + rfm['m'])/2).apply(lambda x: truncate(x))

# COMMAND ----------

rfm.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Segment	Description
# MAGIC * **Champions**	Bought recently, buy often and spend the most
# MAGIC * **Loyal Customers**	Buy on a regular basis. Responsive to promotions.
# MAGIC * **Potential Loyalists**	Recent customers with average frequency.
# MAGIC * **Recent Customers**	Bought most recently, but not often.
# MAGIC * **Promising**	Recent shoppers, but haven’t spent much.
# MAGIC * **Customers Needing Attention**	Above average recency, frequency and monetary values. May not have bought very recently though.
# MAGIC * **About To Sleep**	Below average recency and frequency. Will lose them if not reactivated.
# MAGIC * **At Risk**	Purchased often but a long time ago. Need to bring them back!
# MAGIC * **Can’t Lose Them**	Used to purchase frequently but haven’t returned for a long time.
# MAGIC * **Hibernating**	Last purchase was long back and low number of orders.
# MAGIC * **Lost** Purchased long time ago and never came back.

# COMMAND ----------

# We create a segment map of only 11 segments based on only two scores: 'r' and 'fm'

segment_map = {
    r'22': 'hibernating',
    r'[1-2][1-2]': 'lost',
    r'15': 'can\'t lose',
    r'[1-2][3-5]': 'at risk',
    r'3[1-2]': 'about to sleep',
    r'33': 'need attention',
    r'55': 'champions',
    r'[3-5][4-5]': 'loyal customers',
    r'41': 'promising',
    r'51': 'new customers',
    r'[4-5][2-3]': 'potential loyalists'
}

rfm['segment'] = rfm['r'].map(str) + rfm['fm'].map(str)
rfm['segment'] = rfm['segment'].replace(segment_map, regex=True)
rfm.head()

# COMMAND ----------

rfm.isnull().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploring the customers segments

# COMMAND ----------

rfm['segment'].unique()

# COMMAND ----------

# We take a look on some segments
rfm[rfm['segment']=="can't lose"].sort_values(by='monetary', ascending=False)

# COMMAND ----------

rfm[rfm['segment']=="need attention"].sort_values(by='monetary', ascending=False).head(10)

# COMMAND ----------

rfm[rfm['segment']=='loyal customers'].sort_values(by='monetary', ascending=False).head(10)

# COMMAND ----------

rfm[rfm['segment']=='champions'].sort_values(by='monetary', ascending=False).head(10)

# COMMAND ----------

rfm['monetary'].mean()

# COMMAND ----------

# Customers with monetary over the average that need attention
rfm[(rfm['monetary']>rfm['monetary'].mean()) & (rfm['segment']=='need attention')]\
    .sort_values(by='monetary', ascending=False)

# COMMAND ----------

# Let's do a scatter plot to explore the distribution of customers
# Using 'monetary' as the size of the points, we see that the majority of customers who spend the most also purchase more frequently
plt.style.use('ggplot')
rfm.plot.scatter(x='recency', y='frequency', s=rfm['monetary']*5e-5, figsize=(20,5), c='dodgerblue')
plt.gca().set(xlabel='recency', ylabel='frequency', title='Customer distribution');

# COMMAND ----------

# We export the dataframe to a CSV file for later processing it in Power BI
# (We added the parameter float_format='%.2f' for setting numbers to two decimals)
# rfm.to_csv('rfm_asia.csv', encoding='utf-8', index=False, float_format='%.2f')

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

# Convert the pandas DataFrame to a Spark DataFrame
rfm_spark_df = spark.createDataFrame(rfm)

# Add the 'inserted_at' field to the DataFrame
rfm_spark_df = rfm_spark_df.withColumn("inserted_at", current_timestamp())

# Write the DataFrame to the table, creating the table if it does not exist
rfm_spark_df.write.mode("append").saveAsTable("dev_ds_catalog.hungcq.cust_segm_rfm_asia")
