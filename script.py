#!/usr/bin/env python3.5
# coding=utf-8
#
# Authors:
# Rafael Castro <rafael.castro@skyscanner.net> - 2015





# pyspark --packages com.databricks:spark-csv_2.11:1.4.0

from pyspark.sql import SQLContext
from random import randint

sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('s3://skyscanner-hdtechtalk/fake-deals.csv')
#df.printSchema()
no_dups = df.dropDuplicates(df.columns[1:])  # get columns from header, remove first
# no_dups.show()
randint(0,10)

cols = df.columns[1:]

def build_condition_string(cols, default_deal_value):
    condition = "`"+cols[0]+"`="+str(default_deal_value)
    for col in cols[1:]:
        condition += (" AND `"+col+"`="+str(default_deal_value))
    return condition

#expr = lambda x: x==0.6 for x in cols
#>>> df.filter((df['AL MOBILE']==0.6) & (df['AL DESKTOP']==0.6)).show()


e = build_condition_string(cols, 0.6)
df.filter((df[cols[0]]==0.6) & (df[cols[1]]==0.6)).show()



df
+--------+---------+----------+
|HOTEL ID|AL MOBILE|AL DESKTOP|
+--------+---------+----------+
|   80341|     0.78|       0.7|
|  255836|      0.6|       0.6|
|  245281|     0.78|      0.99|
|  229166|      0.0|       0.7|
+--------+---------+----------+

new
+--------+---------+----------+
|HOTEL ID|AL MOBILE|GB DESKTOP|
+--------+---------+----------+
|   80341|     0.78|       0.7|
|  255836|      0.6|       0.6|
|  245281|     0.1|      0.1  |
|  229166|      0.0|       0.7|
|22944166|      3.0|       4.0|
+--------+---------+----------+

df_semi
+--------+---------+----------+
|HOTEL ID|AL MOBILE|GB DESKTOP|
+--------+---------+----------+
|  255836|      0.6|       0.6|
|  245281|      0.1|       0.1|
|   80341|     0.78|       0.7|
|  229166|      0.0|       0.7|
+--------+---------+----------+

df_join
+--------+---------+----------+--------+---------+----------+
|HOTEL ID|AL MOBILE|GB DESKTOP|HOTEL ID|AL MOBILE|AL DESKTOP|
+--------+---------+----------+--------+---------+----------+
|  255836|      0.6|       0.6|  255836|      0.6|       0.6|
|  245281|      0.1|       0.1|  245281|     0.78|      0.99|
|   80341|     0.78|       0.7|   80341|     0.78|       0.7|
|  229166|      0.0|       0.7|  229166|      0.0|       0.7|
|22944166|      3.0|       4.0|    null|     null|      null|
+--------+---------+----------+--------+---------+----------+

df_semi = df_new.join(df, df_new['HOTEL ID'] == df['HOTEL ID'], 'left_semi')
df_jj = df_new.subtract(df_semi)
+--------+---------+----------+
|HOTEL ID|AL MOBILE|GB DESKTOP|
+--------+---------+----------+
|22944166|      3.0|       4.0|
+--------+---------+----------+

df_sum = df_semi.unionAll(df_jj)
+--------+---------+----------+
|HOTEL ID|AL MOBILE|GB DESKTOP|
+--------+---------+----------+
|  255836|      0.6|       0.6|
|  245281|      0.1|       0.1|
|   80341|     0.78|       0.7|
|  229166|      0.0|       0.7|
|22944166|      3.0|       4.0|
+--------+---------+----------+




df_new_one
+--------+---------+
|HOTEL ID|AL MOBILE|
+--------+---------+
|   80341|     0.78|
|  255836|      0.6|
|  245281|      0.1|
|  229166|      0.0|
|22944166|      3.0|
+--------+---------+

df
+--------+---------+----------+
|HOTEL ID|AL MOBILE|AL DESKTOP|
+--------+---------+----------+
|   80341|     0.78|       0.7|
|  255836|      0.6|       0.6|
|  245281|     0.78|      0.99|
|  229166|      0.0|       0.7|
+--------+---------+----------+

df_s = df_new_one.join(df, df_new_one['HOTEL ID'] == df['HOTEL ID'], 'left_semi')
+--------+---------+
|HOTEL ID|AL MOBILE|
+--------+---------+
|  255836|      0.6|
|  245281|      0.1|
|   80341|     0.78|
|  229166|      0.0|
+--------+---------+

df_old = df.select(df_new_one.columns)  #df.drop('age').collect()
+--------+---------+
|HOTEL ID|AL MOBILE|
+--------+---------+
|   80341|     0.78|
|  255836|      0.6|
|  245281|     0.78|
|  229166|      0.0|
+--------+---------+

then subtract and process by row:



# new columns, add to DIFF and TOTAL
new_cols = df_new.select([x for x in df_new.columns if x not in df.columns]) # with the hotel id column as well
# export resulting into file to be processed immediately

# let's work on the existing ones
df_new = df_new.select([x for x in df_new.columns if x in df.columns])
df_old = df.select([x for x in df.columns if x in df_new.columns])
df_xx = df_new.subtract(df_old) # to be processed

df_old = df_old.subtract(df_new) # will make searching faster??
rows_to_processed = []
columns = df_xx.columns

def is_existing_hotel(df, hotel_id):
    return df['HOTEL ID']==hotel_id

def get_hotel_row(hotel_id, df_old):
    #hotel_id = row['HOTEL ID']
    rows = df_old.filter(is_existing_hotel(df_old, hotel_id)).collect()
    if rows:
        print(rows)




    old
    +--------+---------+----------+
    |HOTEL ID|GB       |US        |
    +--------+---------+----------+
    |   80341|     0.78|       0.7|
    |  255836|      0.6|       0.6|
    |  245281|     0.78|      0.99|
    |  229166|      0.0|       0.7|
    +--------+---------+----------+

    new
    +--------+---------+----------+
    |HOTEL ID|GB       |US        |
    +--------+---------+----------+
    |   80341|     1   |       0.7|
    |  255836|      0.6|       1  |
    |  245281|     0.78|      0.99|
    |  333   |      0.0|       0.7|
    +--------+---------+----------+

    expected result
    +--------+---------+----------+
    |HOTEL ID|GB       |US        |
    +--------+---------+----------+
    |   80341|     1   |      None|
    |  255836|     None|       1  |
    |  333   |      0.0|       0.7|
    +--------+---------+----------+


def get_result(row):
    if row[0] == 'HOTEL ID':
        return None
    new_row = []
    new_row.append(row[0])
    for i in range(1,3):
        if row[i] == row[i+2]:
            new_row.append[-99]
        else:
            new_row.append[row[i+2]]
    print (new_row)



from pyspark.sql import functions as F


ds.withColumn("new_ALM", F.when(ds['AL MOBILE'] == df['AL MOBILE'], "").otherwise(ds['ALM'])).show() # or with ds. too



F.when( (df["col-1"]>0.0) & (df["col-2">0.0), 1).otherwise(0)

df.select(F.when(df['age'] == 2, 3).otherwise(4).alias("age")).collect()


def get_row(row):
    F.when(row['age'] == 2, 3).otherwise(4).alias("age")

new_cols = df_new.select([x for x in df.columns])


ds.select(ds['HOTEL ID'], F.when(ds['AL MOBILE'] == df['AL MOBILE'], "").otherwise(ds['ALM']), F.when(ds['AL DESKTOP'] == df['ALD'], "").otherwise(ds['ALD']))


aa = dd.select(dd['H'], F.when(dd['M'] == dd['MO'], "").otherwise(dd['M']).alias('M'), F.when(dd['D'] == dd['DE'], "").otherwise(dd['D']).alias('D'))

[gg(x) for x in df.columns]

def gg(x, index):
    return F.when(dd[index] == dd[index+2], "").otherwise(dd[index]).alias(index)


def gg(row):
    row_dict = {'HOTEL ID': row[0]}
    l = len(row.__fields__)/2
    for i, field in enumerate(row.__fields__):
        if i < l+1:
            if row[i]==row[i+2]:
                row_dict[field] = None
            else:
                row_dict[field] = row[field]
    return row_dict


# update df[update_col], mapping old_value --> new_value
from pyspark.sql import functions as F
df = df.withColumn(update_col,
    F.when(df[update_col]==old_value,new_value).
    otherwise(df[update_col]))

dg = dd.withColumn('M', F.when(dd['M']==0.6,6).otherwise(dd['M']))

dg.rdd.map(lambda row: gg(row)).collect()
dg.rdd.map(lambda row: gg(row)).toDF().show()

df = reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]), xrange(len(oldColumns)), data)


for i, col in enumerate(df_outer.columns):
    if i> 0 and i < (len(df_outer.columns)/2):
        df_outer = df_outer.withColumn(col,
            F.when(df_outer[col]==df_outer[i+2],None).
            otherwise(df_outer[col]))


for i in range(1, (len(df_outer.columns)/2+1)):
        df_outer = df_outer.withColumn(df_outer[i],
            F.when(df_outer[i]==df_outer[i+2],None).
            otherwise(df_outer[i]))




from pyspark.sql import functions as F

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('s3://skyscanner-hdtechtalk/fake-deals.csv')
df_new = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('s3://skyscanner-hdtechtalk/new-fake-deals.csv')
df_old = df.select([x for x in df.columns if x in df_new.columns])
new_markets = df_new.select([x for x in df_new.columns if (x == 'HOTEL ID' or  x not in df_old.columns)])
df_new = df_new.select([x for x in df_new.columns if (x == 'HOTEL ID' or  x not in new_markets.columns)])
df_old = reduce(lambda df_old, idx: df_old.withColumnRenamed(df_old.columns[idx], df_old.columns[idx]+'a'), xrange(len(df_old.columns)), df_old)
df_outer = df_new.join(df_old, df_new['HOTEL ID'] == df_old['HOTEL IDa'], 'left_outer')
for i, col in enumerate(df_outer.columns):
    if i> 0 and i < (len(df_outer.columns)/2):
        df_outer = df_outer.withColumn(col,
            F.when(df_outer[col]==df_outer[i+2],None).
            otherwise(df_outer[col]))

df_outer.select([x for x in df_new.columns]).show()


