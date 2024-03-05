# Databricks notebook source
#https://datahub.io/core/glacier-mass-balance

# COMMAND ----------

import requests
from pyspark.sql import DataFrame

# COMMAND ----------

with requests.get('https://datahub.io/core/glacier-mass-balance/r/glaciers.csv', stream=True) as r:
    with open('/dbfs/glacier.csv', 'wb') as f:
        for chunk in r.iter_content (chunk_size=8192):
            f.write(chunk)

# COMMAND ----------

def get_data(url:str):
    filename= url.split('/')[-1]
    with requests.get('https://datahub.io/core/glacier-mass-balance/r/glaciers.csv', stream=True) as r:
        with open("/dbfs/{}". format (filename), 'wb') as f:
            for chunk in r.iter_content (chunk_size=8192):
                f.write(chunk)
    return filename

# COMMAND ----------

file_name = get_data('https://datahub.io/core/glacier-mass-balance/r/glaciers.csv')

# COMMAND ----------

file_name

# COMMAND ----------

spark.read.format("csv").option("header","true").load("file:/dbfs/glacier.csv")

# COMMAND ----------

file_format=file_name.split(".")[-1]

# COMMAND ----------

def read_data(file_name) :
    if file_format == 'csv':
        df = spark.read.format(file_format).option("header","true").load("file:/dbfs/{}".format(file_name))
    elif file_format== 'json':
      try:
        df = spark.read.format (file_format).load("file:/dbfs/{}".format(file_name))
      except:
        df = spark.read.format(file_format).option("multiline","true").load("file: /dbfs/{}".format (file_name))
    elif file_format== 'parquet':
      df = spark, read.format (file_format).load("file: /dbfs/{}".format(file_name))
    elif file_format== 'txt':
      df = spark.read.text("file: /dbfs/{}" .format(file_name))
    return df

# COMMAND ----------

df = read_data(file_name)
#spark.read.format("csv").option("header","true").load("file:/dbfs/glacier.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView("df")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view nintys as select * from df where Year like '19%' order by Year asc;
# MAGIC create or replace temp view modern as select * from df where Year like '20%' order by Year asc;

# COMMAND ----------

nintys_df = spark.sql("select * from nintys")
modern_df = spark.sql("select * from modern")

# COMMAND ----------

def transform_data(df: DataFrame) :
    spark.sql("create or replace temp view nintys as select * from df where Year like '19%' order by Year asc;")
    nintys_df = spark.sql("select * from nintys")
    spark.sql("create or replace temp view modern as select * from df where Year like '20%' order by Year asc;")
    modern_df = spark.sql("select * from modern")
    return nintys_df, modern_df

# COMMAND ----------

x, y = transform_data(df)

# COMMAND ----------

 display(y)

# COMMAND ----------

display(x)

# COMMAND ----------

display (nintys_df)

# COMMAND ----------

display (modern_df)

# COMMAND ----------

nintys_file_namez = spark.sql ("(select * from nintys order by Year ASC limit 1) union (select * from nintys order by Year DESC limit 1)")
modern_file_namez = spark.sql("(select * from modern order by Year ASC limit 1) union (select * from modern order by Year DESC limit 1)")

# COMMAND ----------

display (nintys_file_namez)

# COMMAND ----------

display (modern_file_namez)

# COMMAND ----------

modern_file_namez_df = modern_file_namez.collect()

# COMMAND ----------

modern_file_namez_df[0].__getitem__( 'Year') + "-" + modern_file_namez_df [1]. __getitem__('Year')

# COMMAND ----------

def create_file_names():
    nintys_file_namez = spark.sql("(select * from nintys order by Year ASC limit 1) union (select * from nintys order by Year DESC limit 1)")
    modern_file_namez = spark.sql("(select * from modern order by Year ASC limit 1) union (select * from modern order by Year DESC limit 1)")
    nintys_file_namez_df = nintys_file_namez.collect()
    modern_file_namez_df = modern_file_namez.collect()
    nintys_file_name = nintys_file_namez_df[0].__getitem__( 'Year') + "-" + nintys_file_namez_df[1].__getitem__( 'Year')
    modern_file_name = modern_file_namez_df[0]. __getitem__( 'Year') + "-" + modern_file_namez_df[1].__getitem__( 'Year')
    return nintys_file_name, modern_file_name

# COMMAND ----------

m,n = create_file_names()

# COMMAND ----------

print(m,n)

# COMMAND ----------

nintys_df.write.format('parquet').save("/dbfs/nintys_df.parquet")

# COMMAND ----------

def write_df (file_type: str,dfs, file_names):
    for x,y in zip(dfs, file_names) :
      m = x.write.format(file_type).save("/dbfs/{}.{}".format (y, file_type))
    return m

# COMMAND ----------

def write_df (file_type: str,dfs, file_names) :
    for x,y in zip(dfs, file_names) :
      m = x.write.format(file_type).save("/dbfs/{}. {}". format (y, file_type))
    return m

# COMMAND ----------

write_df("parquet", [x, y], [m, n])

# COMMAND ----------

dbutils.fs.ls('/dbfs/')

# COMMAND ----------


