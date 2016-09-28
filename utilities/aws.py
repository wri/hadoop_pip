import json

from utilities import util


def start_emr(instance_type='m3.xlarge', instance_count=3):

    boostrap_script = r"s3://gfw2-data/alerts-tsv/bootstrap.sh"

    cmd = ['aws', 'emr', 'create-cluster', '--release-label', 'emr-5.0.0']
    cmd += ['--instance-type', instance_type, '--instance-count', str(instance_count)]
    cmd += ['--bootstrap-actions', json.dumps([{"Path": boostrap_script, "Name": "Bootstrap action"}])]
    cmd += ['--applications', 'Name=Spark', '--ec2-attributes', json.dumps({"KeyName": "chofmann-wri"})]
    cmd += ['--log-uri', 's3n://gfw2-data/alerts-tsv/output/logs/', '--name', 'Spark PIP cluster']
    cmd += ['--region', 'us-east-1']

    print cmd

    output = util.run_subprocess(cmd)

    cluster_id = util.response_to_dict(output)['cluster_id']

    # wait here until it's ready
    print 'Waiting until cluster is ready'
    cmd = ['']

    return cluster_id

if __name__ == '__main__':
    start_emr(None, 1)