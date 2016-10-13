import os
import subprocess
import uuid
import shutil
from ConfigParser import SafeConfigParser


def read_config(config):

    parser = SafeConfigParser()
    parser.read(config)

    config_dict = {}

    for section in parser.sections():
        config_dict[section] = {}

        for option in parser.options(section):
            config_dict[section][option] = parser.get(section, option)

    return config_dict


def write_bootstrap_file(data_dir, s3_url):

    bootstrap_file = os.path.join(data_dir, 'bootstrap.sh')
    process_job_s3url = '{0}/{1}'.format(s3_url, 'process_job.py')

    with open(bootstrap_file, 'w') as writefile:
        writefile.write('#!/bin/bash\n')
        writefile.write('set -e\n')
        writefile.write('aws s3 cp {0} /home/hadoop/process_job.py\n'.format(process_job_s3url))


def write_properties(config_file, data_dir, s3_url):

    config_dict = read_config(config_file)
    filename = 'application.properties'
    properties_file = os.path.join(data_dir, filename)

    output_s3 = '{0}/{1}'.format(s3_url, 'output.csv')

    if config_dict:

        with open(properties_file, 'wb') as propfile:
            propfile.write('spark.app.name=YARN Points in World\n')
            propfile.write('s3.output={0}'.format(output_s3))

            for section, section_dict in config_dict.iteritems():
                propfile.write('\n#')

                for key, value in section_dict.iteritems():
                    propfile.write('\n{0}.{1}={2}'.format(section, key, value))
    else:
        raise ValueError('Unable to find config file {0}'.format(config_file))

    return output_s3


def copy_process_jobs(root_dir, data_dir):

    # Copy process job script into data dir before we move the whole thing to s3
    process_job_src = os.path.join(root_dir, 'process_job.py')
    process_job_dst = os.path.join(data_dir, 'process_job.py')

    shutil.copy(process_job_src, process_job_dst)


def create(config_file):

    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    guid = str(uuid.uuid4())

    data_dir = os.path.join(root_dir, 'data', guid)
    os.mkdir(data_dir)

    s3_url = r's3://gfw2-data/alerts-tsv/hadoop-jobs/{0}'.format(guid)

    s3_output = write_properties(config_file, data_dir, s3_url)

    copy_process_jobs(root_dir, data_dir)

    write_bootstrap_file(data_dir, s3_url)

    cmd = ['aws', 's3', 'cp', '--recursive', data_dir, s3_url]
    subprocess.check_call(cmd)

    return s3_url, s3_output
