# Databricks notebook source
!pip install -U dataprep numpy pandas markupsafe==2.0.1

# COMMAND ----------

import numpy as np
import pandas as pd
import math
from datetime import timedelta, datetime
from dataprep.clean import clean_country

# COMMAND ----------

df1 = spark.sql("SELECT * FROM `dev_ds_catalog`.`hungcq`.`cust_segm_sales_asia`")
df1.head()

# COMMAND ----------

df1_db = df1
df = df1.toPandas()
df.head()

# COMMAND ----------

# Splitting 'week.year' column on '.' and creating 'week' and 'year' columns
df['week'] = df['week.year'].astype(str).str.split('.').str[0]
df['year'] = df['week.year'].astype(str).str.split('.').str[1]
# Converting year and week into date, using Monday as first day of the week
df['date'] = pd.to_datetime(df['year'].map(str) + df['week'].map(str) + '-1', format='%Y%W-%w')
df.head()

# COMMAND ----------

df['date'].max()

# COMMAND ----------

df['revenue'].max()

# COMMAND ----------

df['revenue'].min()

# COMMAND ----------

df['units'].max()

# COMMAND ----------

df['units'].min()

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import timedelta

# Calculate the size of new_df
new_df_size = int(len(df) * 0.01)

# Selecting countries and ids from df
countries = df['country'].unique()
ids = df['id'].unique()

# Generating country and id columns for new_df
new_df_country = np.random.choice(countries, size=new_df_size)
# Adjusting the generation of new_df_id to ensure the lengths match new_df_size
existing_ids_size = int(new_df_size * 0.9)
new_ids_size = new_df_size - existing_ids_size  # Ensuring the total is exactly new_df_size

new_df_id = np.concatenate([
    np.random.choice(ids, size=existing_ids_size),  # 90% existing ids
    np.random.randint(ids.max() + 1, ids.max() + 1 + new_ids_size, size=new_ids_size)  # Remaining as new ids
])

# Generating week.year column for new_df
max_date = df['date'].max()
next_week_date = max_date + timedelta(days=7)
new_df_week_year = [f"{next_week_date.strftime('%W.%Y')}"] * new_df_size

# Generating revenue and units columns for new_df
revenue_max = df['revenue'].max()
revenue_min = df['revenue'].min()
units_max = df['units'].max()
units_min = df['units'].min()

new_df_revenue = np.random.uniform(revenue_min, revenue_max, size=new_df_size)
new_df_units = np.random.randint(units_min, units_max + 1, size=new_df_size)

# Creating new_df
new_df = pd.DataFrame({
    'country': new_df_country,
    'id': new_df_id,
    'week.year': new_df_week_year,
    'revenue': new_df_revenue,
    'units': new_df_units
})
new_df.head()

# COMMAND ----------

next_week_date_str = next_week_date.strftime('%W.%Y')

# COMMAND ----------

assert new_df['week.year'][0] == next_week_date_str

# COMMAND ----------

# Checking unique values of week.year column
new_df['week.year'].unique()

# COMMAND ----------

assert new_df['week.year'].nunique() == 1

# COMMAND ----------

new_df.isnull().sum()

# COMMAND ----------

assert new_df.isnull().sum().sum() == 0

# COMMAND ----------

new_df.info()

# COMMAND ----------

new_df.describe()

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

# Convert the pandas DataFrame to a Spark DataFrame
spark_df = spark.createDataFrame(new_df)

# Add the 'inserted_at' field to the DataFrame
# spark_df = spark_df.withColumn("inserted_at", current_timestamp())

# Write the DataFrame to the table, creating the table if it does not exist
spark_df.write.mode("append").saveAsTable("dev_ds_catalog.hungcq.cust_segm_sales_asia")
