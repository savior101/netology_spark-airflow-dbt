from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pandas as pd

spark = SparkSession.builder.appName('hw-spark_sql').getOrCreate()

df = spark.read.option('header', True).option('sep',',').option('inferSchema', True).csv('owid-covid-data.csv')

'''
1. Выберите 15 стран с наибольшим процентом переболевших на 31 марта
(в выходящем датасете необходимы колонки:iso_code, страна, процент переболевших)
'''
first_df = df.select('iso_code', 'location',
                     ((col('total_cases') / col('population')) * 100).alias('Процент заболевших'))\
    .filter(col('date') == '2021-03-31')\
    .filter(col('iso_code').startswith('OWID_') == False)\
    .orderBy(((col('total_cases') / col('population')) * 100).desc())\
    .limit(15)\
    .show()
'''
Ответ:
+--------+-------------+------------------+
|iso_code|     location|Процент заболевших|
+--------+-------------+------------------+
|     AND|      Andorra|15.543907331909661|
|     MNE|   Montenegro|14.523725364693293|
|     CZE|      Czechia|14.308848404077997|
|     SMR|   San Marino|13.937179562732041|
|     SVN|     Slovenia|10.370805779121204|
|     LUX|   Luxembourg| 9.847342390123583|
|     ISR|       Israel| 9.625106044786802|
|     USA|United States| 9.203010995860707|
|     SRB|       Serbia| 8.826328557933492|
|     BHR|      Bahrain| 8.488860079114566|
|     PAN|       Panama| 8.228739065460761|
|     PRT|     Portugal| 8.058699735120369|
|     EST|      Estonia| 8.022681579659551|
|     SWE|       Sweden| 7.969744347858805|
|     LTU|    Lithuania| 7.938864728274825|
+--------+-------------+------------------+
'''


'''
2. Top 10 стран с максимальным зафиксированным кол-вом новых случаев за последнюю неделю марта 2021
в отсортированном порядке по убыванию (в выходящем датасете необходимы колонки: число, страна, кол-во новых случаев)
'''
windowSpec = Window.partitionBy('location').orderBy(col('new_cases').desc())
second_df = df.select('date', 'location', 'new_cases')\
    .filter(col('date').between(pd.to_datetime('2021-03-25'), pd.to_datetime('2021-03-31')))\
    .filter(col('iso_code').startswith('OWID_') == False)\
    .withColumn('new_cases_rr', row_number().over(windowSpec))\
    .filter(col('new_cases_rr') == 1)\
    .drop('new_cases_rr')\
    .orderBy(col('new_cases').desc())\
    .limit(10)\
    .show()

'''
Ответ:
+-------------------+-------------+---------+
|               date|     location|new_cases|
+-------------------+-------------+---------+
|2021-03-25 00:00:00|       Brazil| 100158.0|
|2021-03-26 00:00:00|United States|  77321.0|
|2021-03-31 00:00:00|        India|  72330.0|
|2021-03-31 00:00:00|       France|  59054.0|
|2021-03-31 00:00:00|       Turkey|  39302.0|
|2021-03-26 00:00:00|       Poland|  35145.0|
|2021-03-31 00:00:00|      Germany|  25014.0|
|2021-03-26 00:00:00|        Italy|  24076.0|
|2021-03-25 00:00:00|         Peru|  19206.0|
|2021-03-26 00:00:00|      Ukraine|  18226.0|
+-------------------+-------------+---------+
'''


'''
3. Посчитайте изменение случаев относительно предыдущего дня в России за последнюю неделю марта 2021.
(например: в россии вчера было 9150 , сегодня 8763, итог: -387)
(в выходящем датасете необходимы колонки: число, кол-во новых случаев вчера, кол-во новых случаев сегодня, дельта)
'''
windowSpec = Window.orderBy('date')
third_df = df.select('date', 'new_cases')\
    .filter(col('location') == 'Russia')\
    .filter(col('date').between(pd.to_datetime('2021-03-25'), pd.to_datetime('2021-03-31'))) \
    .withColumn('yesterday_new_cases', lag('new_cases', 1).over(windowSpec)) \
    .withColumn('delta', col('new_cases') - col('yesterday_new_cases'))\
    .show()

spark.stop()
'''
Ответ:
+-------------------+---------+-------------------+------+
|               date|new_cases|yesterday_new_cases| delta|
+-------------------+---------+-------------------+------+
|2021-03-25 00:00:00|   9128.0|               null|  null|
|2021-03-26 00:00:00|   9073.0|             9128.0| -55.0|
|2021-03-27 00:00:00|   8783.0|             9073.0|-290.0|
|2021-03-28 00:00:00|   8979.0|             8783.0| 196.0|
|2021-03-29 00:00:00|   8589.0|             8979.0|-390.0|
|2021-03-30 00:00:00|   8162.0|             8589.0|-427.0|
|2021-03-31 00:00:00|   8156.0|             8162.0|  -6.0|
+-------------------+---------+-------------------+------+
'''