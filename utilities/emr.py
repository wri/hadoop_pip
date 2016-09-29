import subprocess

from utilities import util


def start(s3_app_folder, instance_type='m3.xlarge', instance_count=3):

    bootstrap_script = r"{0}/bootstrap.sh".format(s3_app_folder)

    cmd = ['aws', 'emr', 'create-cluster', '--name', 'Spark PIP cluster']
    cmd += ['--release-label', 'emr-5.0.0', '--applications', 'Name=Spark']
    cmd += ['--ec2-attributes', 'KeyName=chofmann-wri']
    cmd += ['--instance-type', instance_type, '--instance-count', str(instance_count)]
    cmd += ['--use-default-roles', '--region', 'us-east-1']
    cmd += ['--log-uri', 's3n://gfw2-data/alerts-tsv/output/logs/']
    cmd += ['--enable-debugging']
    cmd += ['--bootstrap-action', 'Path="{0}"'.format(bootstrap_script)]

    output = util.run_subprocess(cmd)
    cluster_id = util.response_to_dict(output)['ClusterId']

    print cmd

    print "Started cluster {0}, waiting until it's ready".format(cluster_id)
    cmd = ['aws', 'emr', 'wait', 'cluster-running', '--cluster-id', cluster_id]

    subprocess.check_call(cmd)

    return cluster_id


def add_step(cluster_id, s3_app_folder):

    properties_file_s3url = '{0}/application.properties'.format(s3_app_folder)

    cmd = ['aws', 'emr', 'add-steps' '--cluster-id', cluster_id]
    cmd += ['--steps Type=Spark', 'Name=SparkSQL', 'ActionOnFailure=CONTINUE']
    cmd += ['Args=[/home/hadoop/process_job.py, {0}]'.format(properties_file_s3url)]

    output = util.run_subprocess(cmd)
    step_id = util.response_to_dict(output)['StepIds'][0]

    return step_id


def monitor_step(cluster_id, step_id):

    cmd = ['aws', 'emr', 'describe-step' '--cluster-id', cluster_id, '--step-id', step_id]
    output = util.run_subprocess(cmd)
    step_status = util.response_to_dict(output)['Step']['Status']['State']

    print 'Step status is {0}'.format(step_status)

    return step_status


def terminate(cluster_id):

    subprocess.check_call(['aws', 'emr', 'terminate-clusters', '--cluster-ids', cluster_id])
