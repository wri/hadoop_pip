import os
import subprocess
import csv
import sys

app_properties_s3_url = sys.argv[1]
target_zip_s3_url = r's3://gfw2-data/alerts-tsv/target.zip'

for download in [app_properties_s3_url, target_zip_s3_url]:
    cmd = ['aws', 's3', 'cp', download, '.']
    subprocess.check_call(cmd)

os.environ["SPARK_HOME"] = r"/usr/lib/spark"

# Set PYTHONPATH for Spark
for path in [r'/usr/lib/spark/python/', r'/usr/lib/spark/python/lib/py4j-src.zip']:
    sys.path.append(path)

# unzip our jars
subprocess.check_call(['unzip', '-o', 'target.zip'])

# clear out the output dir in hdfs just in case
# don't check_call, assume if an error that it doesn't exist
subprocess.call(['hdfs', 'dfs', '-rm', '-r', 'output'])

pip_cmd = ['spark-submit', '--master', 'yarn-client', '--driver-memory', '16g']
pip_cmd += ['--executor-memory', '4g', '--executor-cores', '1']
pip_cmd += ['--driver-java-options', '-server -Xms1g']
pip_cmd += ['--jars', r'target/libs/jts-1.13.jar', 'target/spark-pip-0.1.jar']

subprocess.check_call(pip_cmd)

# Summarize results
from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext()
sqlContext = SQLContext(sc)

df = sqlContext.read.csv('hdfs:///user/hadoop/output/', sep=',', inferSchema=True)
df.registerTempTable("my_table")

# grab SQL query from properties file
with open('application.properties', 'r') as readfile:
    for line in readfile:
        if line[0:9] == 'sql.query':
            query = line.split('=')[1].strip()
        if line[0:9] == 's3.output':
            s3_output_path = line.split('=')[1].strip()

query1 = sqlContext.sql(query)
local_csv = r'/home/hadoop/query1.csv'

with open(local_csv, 'w') as output:
    csv_writer = csv.writer(output)
    for out_row in query1.collect():
        csv_writer.writerow(out_row)

subprocess.check_call(['aws', 's3', 'cp', local_csv, s3_output_path])

sc.stop()
