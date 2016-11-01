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

query_dict = {}

# grab SQL query from properties file
with open('application.properties', 'r') as readfile:
    for line in readfile:
        if line[0:3] == 'sql':

            # Grab the digit from the string 'query1' or query22 or whatever
            query_id = ''.join([char for char in line.split('=')[0] if char.isdigit()])
            query_int = int(query_id)

            # Grab whatever this key is equal to
            value = '='.join(line.split('=')[1:]).strip()

            if line[0:9] == 'sql.query':
                param = 'sql'
            elif line[0:10] == 'sql.output':
                param = 's3_output'
            else:
                param = 'header_row'

            # Build a dict of queries, each having an {id: {sql: '', 's3_output': ''}}
            try:
                query_dict[query_int][param] = value
            except KeyError:
                query_dict[query_int] = {param: value}

for qry_id, qry_params in query_dict.iteritems():

    spark_query = sqlContext.sql(qry_params['sql'])

    local_csv = r'/home/hadoop/query1.csv'

    with open(local_csv, 'w') as output:
        csv_writer = csv.writer(output)

        # Write CSV header if it exists
        try:
            header_text = qry_params['header_row']
            header_row = [s.strip() for s in header_text.split(',')]
            csv_writer.writerow(header_row)

        except KeyError:
            pass

        for out_row in spark_query.collect():
            csv_writer.writerow(out_row)

    subprocess.check_call(['aws', 's3', 'cp', local_csv, qry_params['s3_output']])

sc.stop()
