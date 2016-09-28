import argparse

from utilities import aws, util

parser = argparse.ArgumentParser(description='Run point and polygon using AWS EMR')
parser.add_argument('--config', '-c', nargs='+', help='config file of parameters', required=True)
args = parser.parse_args()

config_dict = util.read_config(args.config)

# Start emr and wait until it's read
cluster_id = aws.start_emr(instance_count=1)

# create an S3 dir with guid

# write new application.properties

# add step with process stop and application.properties path argument
# on the EMR machine, process_jobs.py goes and gets application.properties, reads info, starts the job
# also reads extra stuff from application.properties-- sql statements etc for later in the process

# check on step every minute
# once it's done, add another step or terminate

# post process results
# also in glad.cfg -- include postprocessing steps -- summarize data and push to api


