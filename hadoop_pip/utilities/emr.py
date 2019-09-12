import boto3
import click
import subprocess
import util

CLIENT = boto3.client('emr')


def start(s3_app_folder, instance_type='m4.2xlarge', instance_count=3, name='Spark PIP cluster'):
    bootstrap_script = r"{0}/bootstrap.sh".format(s3_app_folder)

    response = CLIENT.run_job_flow(
        Name=name,
        LogUri=s3_app_folder,
        ReleaseLabel='emr-5.26.0',
        Applications=[
            {
                'Name': 'Spark',
            },
            {
                'Name': 'Hadoop',
            },
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm4.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': "Workers",
                    'Market': 'SPOT',
                    'InstanceRole': 'CORE',
                    'InstanceType': instance_type,
                    'InstanceCount': instance_count,
                }
            ],
            'Ec2KeyName': 'tmaschler_wri2',
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-08458452c1d05713b',
        },

        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        BootstrapActions=[

            {"Name" : "Bootstrap action",
                'ScriptBootstrapAction': {
                    'Path': "{0}".format(bootstrap_script),

                }
            },
        ],
        Tags=[
            {
                'Key': 'Project',
                'Value': 'Global Forest Watch',
            },
            {
                'Key': 'Project Lead',
                'Value': 'Thomas Maschler',
            },
            {
                'Key': 'Pricing',
                'Value': 'On Demand',
            },
            {
                'Key': 'Job',
                'Value': 'Compute country stats - {}'.format(name),
            },
            {
                'Key': 'Name',
                'Value': '{}'.format(name),
            },
        ],
    )

    cluster_id = response['JobFlowId']

    click.echo("Started cluster {0}, waiting until it's ready".format(cluster_id))

    waiter = CLIENT.get_waiter('cluster_running')
    waiter.wait(ClusterId=cluster_id)

    return cluster_id


def add_step(cluster_id, s3_app_folder):
    properties_file_s3url = '{0}/application.properties'.format(s3_app_folder)

    response = CLIENT.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                'Name': 'SparkSQL',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {

                    'Jar': "command-runner.jar",

                    'Args': [
                        "spark-submit",
                        "--deploy-mode",
                        "client",
                        "/home/hadoop/process_job.py",
                        properties_file_s3url,
                    ]
                }
            },
        ]
    )

    step_id = response["StepIds"][0]
    return step_id


def monitor_step(cluster_id, step_id):

    response = CLIENT.describe_step(
        ClusterId=cluster_id,
        StepId=step_id
    )

    step_status = response['Step']['Status']['State']

    click.echo('Step status is {0}'.format(step_status))

    return step_status


def terminate(cluster_id):
    subprocess.check_call(['aws', 'emr', 'terminate-clusters', '--cluster-ids', cluster_id])
