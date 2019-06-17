import subprocess

import util


def start(s3_app_folder, instance_type='m3.2xlarge', instance_count=3, name='Spark PIP cluster'):

    bootstrap_script = r"{0}/bootstrap.sh".format(s3_app_folder)

    cmd = ['aws', 'emr', 'create-cluster', '--name', name,
           '--release-label', 'emr-5.5.0', '--applications', 'Name=Spark', 'Name=Hadoop',
           '--ec2-attributes', 'KeyName=tmaschler_wri2',
           '--instance-groups', 'InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge',
           'InstanceGroupType=CORE,InstanceCount={},InstanceType={},BidPrice=4'.format(str(instance_count), instance_type),
           '--use-default-roles', '--region', 'us-east-1',
           '--log-uri', s3_app_folder, '--tags', 'Project=Global Forest Watch', 'Project Lead=Thomas Maschler',
           'Pricing=On Demand', 'Job=Compute country stats - {}'.format(name), 'Name={}'.format(name),
           '--enable-debugging',
           '--bootstrap-action', 'Path="{0}"'.format(bootstrap_script)]

    output = util.run_subprocess(cmd)
    cluster_id = util.response_to_dict(output)['ClusterId']

    print cmd

    print "Started cluster {0}, waiting until it's ready".format(cluster_id)
    cmd = ['aws', 'emr', 'wait', 'cluster-running', '--cluster-id', cluster_id]

    subprocess.check_call(cmd)

    return cluster_id


def add_step(cluster_id, s3_app_folder):

    properties_file_s3url = '{0}/application.properties'.format(s3_app_folder)

    step_args = ['Type=Spark', 'Name=SparkSQL', 'ActionOnFailure=CONTINUE',
                 'Args=[/home/hadoop/process_job.py,{0}]'.format(properties_file_s3url)]

    cmd = ['aws', 'emr', 'add-steps', '--cluster-id', cluster_id,
           '--steps', ','.join(step_args)]

    print cmd

    output = util.run_subprocess(cmd)
    step_id = util.response_to_dict(output)['StepIds'][0]

    return step_id


def monitor_step(cluster_id, step_id):

    cmd = ['aws', 'emr', 'describe-step', '--cluster-id', cluster_id, '--step-id', step_id]
    output = util.run_subprocess(cmd)
    step_status = util.response_to_dict(output)['Step']['Status']['State']

    print 'Step status is {0}'.format(step_status)

    return step_status


def terminate(cluster_id):

    subprocess.check_call(['aws', 'emr', 'terminate-clusters', '--cluster-ids', cluster_id])
