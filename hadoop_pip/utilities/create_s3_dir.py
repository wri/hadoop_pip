import boto3
import click
import os
import uuid
import shutil
import errno
from ConfigParser import SafeConfigParser

S3 = boto3.client('s3')

def read_config(config):

    parser = SafeConfigParser()
    parser.read(config)

    config_dict = {}

    for section in parser.sections():
        config_dict[section] = {}

        for option in parser.options(section):
            config_dict[section][option] = parser.get(section, option)

    # Make sure that config read was successful
    if not config_dict:
        raise ValueError('Unable to find config file {0}'.format(config))

    return config_dict


def write_bootstrap_file(data_dir, s3_url):

    bootstrap_file = os.path.join(data_dir, 'bootstrap.sh')
    process_job_s3url = '{0}/{1}'.format(s3_url, 'process_job.py')

    with open(bootstrap_file, 'w') as writefile:
        writefile.write('#!/bin/bash\n')
        writefile.write('set -e\n')
        writefile.write('aws s3 cp {0} /home/hadoop/process_job.py\n'.format(process_job_s3url))


def add_query(section_dict, section, key, prop_file, s3_job_url):

    try:
        query_id = int(key.replace('query', ''))

    except ValueError:
        raise ValueError('Queries in the config file must be named query{number}')

    try:
        output_key = 'output{0}'.format(query_id)
        output_file = section_dict[output_key]

    except KeyError:
        click.echo('No output file specified for query{0}, defaulting to output{0}.csv'.format(query_id))

        output_file = '{0}/output{1}.csv'.format(s3_job_url, query_id)
        prop_file.write('\n{0}.output{1}={2}'.format(section, query_id, output_file))

    return query_id, output_file


def write_properties(config_file, data_dir, s3_job_url):

    config_dict = read_config(config_file)
    properties_file = os.path.join(data_dir, 'application.properties')

    output_s3_dict = {}

    with open(properties_file, 'wb') as prop_file:
        prop_file.write('spark.app.name=YARN Points in World')

        for section, section_dict in config_dict.iteritems():
            prop_file.write('\n#')

            for key, value in section_dict.iteritems():
                prop_file.write('\n{0}.{1}={2}'.format(section, key, value))

                # For each query specified, check if there's an output defined
                if section == 'sql' and 'query' in key:

                    query_id, query_output = add_query(section_dict, section, key, prop_file, s3_job_url)
                    output_s3_dict[query_id] = query_output

    # Sort the keys to this dict (1, 2, 3, etc) and return the s3 outputs in that order
    output_s3_list = [output_s3_dict[k] for k in sorted(output_s3_dict.keys())]

    # if no queries were specified, likely doing all processing in scala
    # still want to return an output though
    if not output_s3_list:
        output_s3_list = ['{0}/output.csv'.format(s3_job_url)]

    return output_s3_list


def copy_process_jobs(root_dir, data_dir):

    # Copy process job script into data dir before we move the whole thing to s3
    process_job_src = os.path.join(root_dir, 'process_job.py')
    process_job_dst = os.path.join(data_dir, 'process_job.py')

    shutil.copy(process_job_src, process_job_dst)


def create(config_file):

    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # sys.prefix
    guid = str(uuid.uuid4())

    data_dir = os.path.join(root_dir, 'data', guid)
    mkdir_p(data_dir)

    bucket = "gfw2-data"
    prefix = 'alerts-tsv/hadoop-jobs/{0}'.format(guid)
    s3_job_url = "s3://{}/{}".format(bucket, prefix)

    s3_output = write_properties(config_file, data_dir, s3_job_url)

    copy_process_jobs(root_dir, data_dir)

    write_bootstrap_file(data_dir, s3_job_url)

    click.echo("Upload to S3")
    for (root, dirs, files) in os.walk(data_dir):
        for f in files:
            file_name = os.path.join(root,f).replace("\\", "/")
            S3.upload_file(file_name, bucket, "{}/{}".format(prefix, f))

    return s3_job_url, s3_output


def mkdir_p(path):
    # copied from https://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise