import argparse
import time

from utilities import emr, create_s3_dir

parser = argparse.ArgumentParser(description='Run point and polygon using AWS EMR')
parser.add_argument('--config', '-c', help='config file of parameters', required=True)
args = parser.parse_args()

# Write application.properties, bootstrap.sh, and process_job.py to s3_app_folder
s3_app_folder = create_s3_dir.create(args.config)

# Start emr and wait until it's read
cluster_id = emr.start(s3_app_folder)

step_id = emr.add_step(cluster_id, s3_app_folder)

for i in range(0, 1000):
    step_status = emr.monitor_step(cluster_id, step_id)

    if step_status not in ["PENDING", "RUNNING"]:
        break
    else:
        time.sleep(60)

    print 'Step {0} finished with status {1}'.format(step_id, step_status)

emr.terminate(cluster_id)
