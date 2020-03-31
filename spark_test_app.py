# ----------------------------------------spark_test_app.py---------------------------------------
# To Start
# (in from folder project)
# Run command: PYTHONSTARTUP=spark_test_app.py pyspark 
#
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName( '**Spark Test App**' )
#.set("spark.executor.memory", "5g")
sc = SparkContext.getOrCreate(conf)

import re
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DateType

print('\n + Load file: NASA_access_log_Jul95.gz')
# ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
rdd_Jul95 = sc.textFile( 'ftp://anonymous:anonymous@ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz' ).cache()
# count() 1891715
print('   Lines: ' + str(rdd_Jul95.count()))
print('...Top 3 em Jul95')
for i in rdd_Jul95.take(3): print(i)

print('\n + Load file: NASA_access_log_Aug95.gz')
# ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz
rdd_Aug95 = sc.textFile( 'ftp://anonymous:anonymous@ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz' ).cache()
# count(): 1569898 
print('   Lines: ' + str(rdd_Aug95.count()))
print('...Top 3 em Aug95') 
for i in rdd_Aug95.take(3): print(i)

print('\n ! Layout dos arquivos são similares, faremos merge e aplicar o regex')

print('\n - Merging Jul95 and Aug95') 
rdd = sc.union([rdd_Jul95, rdd_Aug95]).cache()
# count() 3461613
print('   Lines in RDD: ' + str(rdd.count()))

print('\n- Regex test')
pattern = r"""([^\s]+)(.*\"\s)([\d\s]+.*)$"""
for n, i in enumerate(rdd.map(lambda x: re.split(pattern , x)[1:-1]).first(), start=1): print(str(n)+']',i)

print('\n - Create DataFrame with regex split')
df = rdd.map(lambda x: re.split(pattern , x)[1:-1]).filter(lambda x: len(x) == 3).toDF(["HOST", "DTHRURL", "RESPONSESIZE"])
# count(): 3461612
print('   Lines in DataFrame: ' + str(df.count()))
df.show(3)

print('\n - Set columns: RESPONSE, SIZE')
df = df.withColumn('Splitted', F.split(df['RESPONSESIZE'], '\s' )).withColumn( 'RESPONSE', F.col( 'Splitted')[0]).withColumn('SIZE', F.col('Splitted')[1])
df.show(3)

print('\n - Fix column: SIZE')
df = df.withColumn("SIZE", df["SIZE"].cast(IntegerType()))
df = df.na.fill(0)
df.show(3)

print('\n - View RESPONSE values')
df.createOrReplaceTempView("dftable")
spark.sql("SELECT RESPONSE, SUM(1) AS QTD FROM dftable GROUP BY RESPONSE ORDER BY 2 DESC").show(5)

print('\n - Set column: DT')
df = df.withColumn("DT", F.regexp_extract(df['DTHRURL'], """(\d{2}\/\D{3}\/\d{4})""",1))
df = df.withColumn("DT", F.to_date( df['DT'], "dd/MMM/yyyy" ))
df.show(3)

print('\n - LAST DATAFRAME HERE:')
df.show(5)

print('\n!!! CALCULATE ANSWERS !!!')

print('\n1. Número de hosts únicos?')
tt_dist = df.select("HOST").distinct().count()
print('   '+str(tt_dist))

print('\n2. O total de erros 404?')
tt_404 = df.filter(df["RESPONSE"]=='404').count()
print('   '+str(tt_404))

print('\n3. Os 5 URLs que mais causaram erro 404?')
df.createOrReplaceTempView("dftable")
top5 = spark.sql("SELECT HOST, SUM(1) AS QTD FROM dftable WHERE RESPONSE='404' GROUP BY HOST ORDER BY 2 DESC LIMIT 5").collect()
tt_top5 = '\n   '.join('('+str(str(n)+') '+i['HOST']+' = '+str(i['QTD'])) for n, i in enumerate(top5, start=1))
print('   '+str(tt_top5))

print('\n4. Quantidade de erros 404 por dia (média por dia):')
tt_avg = spark.sql("SELECT SUM(1)/COUNT(DISTINCT DT) AS QTD_AVG FROM dftable WHERE RESPONSE='404'").collect()
tt_avg = int(tt_avg[0][0])
print('   '+str(tt_avg))
df_day = spark.sql("SELECT DT, SUM(1) AS QTD FROM dftable WHERE RESPONSE='404' GROUP BY DT ORDER BY 1").toDF('DATA','QTD').cache()
df_day.coalesce(1).write.option("header", "true").mode("overwrite").format("csv").save('spark_test_404_day')
print('   (by day in file: "spark_test_404_day")')

print('\n5. O total de bytes retornados?')
tt_size = df.agg(F.sum("SIZE")).collect()[0][0]
print('   '+str(tt_size))

print('Finish program\n')

exit(input('...PRESS ANY KEY TO EXIT...'))
exit(0)

# ----------------------------------------spark_test_app.py---------------------------------------
