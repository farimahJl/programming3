import csv
from pyspark.sql.functions import desc, avg, split, countDistinct, explode
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import split, col

output = []

def explain(expr):
    return expr._sc._jvm.PythonSQLUtils.explainString(expr._jdf.queryExecution(), 'simple')

# Start spark session and read csv
spark = SparkSession.builder.master("local[16]").getOrCreate()
file = '/data/dataprocessing/interproscan/all_bacilli.tsv'
df = spark.read.csv(file, sep=r'\t', header=False, inferSchema= True)

#Q1
# distinct protein annotations
dpa = df.select(countDistinct("_c11"))
unique_number = dpa.take(1)[0]['count(DISTINCT _c11)']
output.append([1, unique_number, explain(dpa)])

#Q2
df2 = df.groupBy("_c0").count().agg({'count':'mean'})
result = df2.take(1)[0]['avg(count)']
output.append([2, result, explain(df2)])

#Q3
df2 = df.filter("_c13 != '-'")
df2 = df2.select(split(col("_c13"),"\|").alias('GO'))
df2 = df2.select(explode(df2.GO))
df2 = df2.groupBy("col").count().sort(col("count"), ascending=False)
result = df2.take(1)[0]['col']
output.append([3, result, explain(df2)])

#Q4
df2 = df.select((col("_c7") - col("_c6")).alias('Diff'))
df2 = df2.agg({'Diff' : 'mean'})
result = df2.take(1)[0]['avg(Diff)']
output.append([4, result, explain(df2)])

#Q5
df2 = df.filter("_c11 != '-'")
df2 = df2.groupBy("_c11").count().sort(col("count"), ascending=False)
result = []
for res in df2.take(10):
    result.append(res[0])
output.append([5, result, explain(df2)])

#Q6
df2 = df.filter("_c11 != '-'")
df2 = df2.select("_c11",((col("_c7")-col("_c6"))/col("_c2")).alias("Pro"))
df2 = df2.filter("Pro >= 0.9").sort("Pro", ascending=False)
result = []
for res in df2.take(10):
    result.append(res[0])
output.append([6, result, explain(df2)])

#Q7
df2 = df.filter("_c12 != '-'")
df2 = df2.select(split(col("_c12"),"\/| |-|,").alias('W'))
df2 = df2.select(explode(df2.W))
df2 = df2.filter("col != ''")
df2 = df2.groupBy("col").count().sort(col("count"), ascending=False)
result = []
for res in df2.take(10):
    result.append(res[0])
output.append([7, result, explain(df2)])

#Q8
df2 = df.filter("_c12 != '-'")
df2 = df2.select(split(col("_c12"),"\/| |-|,").alias('W'))
df2 = df2.select(explode(df2.W))
df2 = df2.filter("col != ''")
df2 = df2.groupBy("col").count().sort(col("count"), ascending=True)
result = []
for res in df2.take(10):
    result.append(res[0])
output.append([8, result, explain(df2)])

#Q9
df2 = df.filter("_c11 != '-'")
df2 = df2.select("_c11", "_c12",((col("_c7")-col("_c6"))/col("_c2")).alias("Pro"))
df2 = df2.filter("Pro >= 0.9").sort("Pro", ascending=False)
df2 = df2.limit(10)
df2 = df2.filter("_c12 != '-'")
df2 = df2.select(split(col("_c12"),"\/| |-|,").alias('W'))
df2 = df2.select(explode(df2.W))
df2 = df2.filter("col != ''")
df2 = df2.groupBy("col").count().sort(col("count"), ascending=False)
result = []
for res in df2.take(10):
    result.append(res[0])
output.append([9, result, explain(df2)])

#Q10
df2 = df.filter("_c11 != '-'")
df2 = df2.select("_c0", "_c11", "_c2")
df2 = df2.groupBy("_c0", "_c2").agg({"_c11":"count"})

R2 = df2.stat.corr("_c2", "count(_c11)") ** 2
result = R2
output.append([10, result, explain(df2)])

with open("output.csv", 'w') as file:
    writer = csv.writer(file)
    writer.writerow(['Question Number', 'Result', 'Spark Explain'])
    for out in output:
        writer.writerow(out)

