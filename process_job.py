import os
import subprocess
import csv
import sys
import datetime


os.environ["SPARK_HOME"] = r"/usr/lib/spark"

# Set PYTHONPATH for Spark
for path in [r'/usr/lib/spark/python/', r'/usr/lib/spark/python/lib/py4j-src.zip']:
    sys.path.append(path)

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import *


def main():

    app_properties_s3_url = sys.argv[1]

    run_spark_pip(app_properties_s3_url)

    query_dict, full_export = build_query_dict()

    if query_dict or full_export:
        summarize_results(query_dict, full_export)

    # if we haven't specified a SQL statement, likely doing all
    # our processing within scala
    else:
        export_result(app_properties_s3_url)


def run_spark_pip(app_properties_s3_url):

    target_zip_s3_url = r's3://gfw2-data/alerts-tsv/target_0.3.zip'

    for download in [app_properties_s3_url, target_zip_s3_url]:
        cmd = ['aws', 's3', 'cp', download, '.']
        subprocess.check_call(cmd)

    # unzip our jars
    subprocess.check_call(['unzip', '-o', 'target_0.3.zip'])

    # clear out the output dir in hdfs just in case
    # don't check_call, assume if an error that it doesn't exist
    subprocess.call(['hdfs', 'dfs', '-rm', '-r', 'output'])

    pip_cmd = ['spark-submit', '--master', 'yarn']
    pip_cmd += ['--executor-memory', '9g']
    pip_cmd += ['--jars', r'target/libs/jts-core-1.14.0.jar', 'target/spark-pip-0.3.jar']

    subprocess.check_call(pip_cmd)


def build_query_dict():

    query_dict = {}
    full_export = None

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

            if line[0:11] == 'export_type':
                full_export = '='.join(line.split('=')[1:]).strip()

    return query_dict, full_export


def summarize_results(query_dict, full_export):

    # use yarn to manage the process, with 9g of mem per executor
    conf = SparkConf().setMaster("yarn")
    conf.set("spark.executor.memory","9g")

    sc = SparkContext()
    sqlContext = SQLContext(sc)

    df = sqlContext.read.csv('hdfs:///user/hadoop/output/', sep=',', inferSchema=True)
    df.registerTempTable("my_table")

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

        # delete this to save space-- not much room in hadoop machine local disk
        os.remove(local_csv)

    if full_export:

        today = datetime.datetime.today().strftime('%Y%m%d')

        if full_export == 'glad':

            # find date of day 180 days ago
            today_date = datetime.date.today()
            glad_back_date = today_date - datetime.timedelta(days=181)
            glad_back_date_jd = glad_back_date.timetuple().tm_yday

            # filtered = df.filter(df['_c3'] > 2016)

            # check of that day is in the current year
            if today_date.year == glad_back_date.year:
                filtered = df.filter((df['_c8'] == 'gadm28') & (df['_c4'] > glad_back_date_jd))

            # otherwise select all dates in the current year, and only those in previous year that are > the julian_day
            else:
                filtered = df.filter(
                    ((df['_c8'] == 'gadm28') & (df['_c3'] == today_date.year - 1) & (df['_c4'] > glad_back_date_jd)) |
                    ((df['_c8'] == 'gadm28') & (df['_c3'] == today_date.year))
                )

            udfValueToCategory = udf(value_to_category, StringType())
            df_with_cat = filtered.withColumn("category", udfValueToCategory("_c2"))

            # filter DF to select only columns of interest
            column_aois = [0, 1, 2, 3, 4, 11, 12, 13, 14]
            df_final = df_with_cat.select(*(df_with_cat.columns[i] for i in column_aois))

            s3_temp_dir = r's3://gfw2-data/alerts-tsv/temp/output-glad-{}/'.format(today)
            s3_dest_dir = r's3://gfw2-data/alerts-tsv/temp/output-glad-summary-{}/'.format(today)

        # terrai - polyname field is _c4 in this case
        else:
            df_final = df.filter(df['_c4'] == 'gadm28')

            s3_temp_dir = r's3://gfw2-data/alerts-tsv/temp/output-{}-{}/'.format(full_export, today)
            s3_dest_dir = r's3://gfw2-data/alerts-tsv/temp/output-{}-summary-{}/'.format(full_export, today)

        # write hadoop output in parts to S3
        df_final.write.csv(s3_temp_dir)

        # group all output files into one
        s3_cmd = ['s3-dist-cp', '--src', s3_temp_dir, '--dest', s3_dest_dir, '--groupBy', '.*(part-r*).*']
        subprocess.check_call(s3_cmd)

        # remove the temp directory
        remove_temp_cmd = ['aws', 's3', 'rm', s3_temp_dir, '--recursive']
        subprocess.check_call(remove_temp_cmd)

    sc.stop()


def export_result(s3_app_properties_url):

    # in this instance, we've written a complete table to hdfs
    # no need to postprocess / further summarize
    # first, copy it locally
    subprocess.check_call(['hdfs', 'dfs', '-getmerge', 'output/', 'out.csv'])

    # then copy it up to s3
    s3_output = s3_app_properties_url.replace('application.properties', 'output.csv')
    subprocess.check_call(['aws', 's3', 'mv', 'out.csv', s3_output])


def value_to_category(value):
    if value == 2:
        return 'unconfirmed'
    elif value == 3:
        return 'confirmed'
    else:
        return '-9999'


if __name__ == '__main__':
    main()

