import argparse
import time

from utilities import emr, create_s3_dir


def run(config_file_list):

    cluster_id = None
    output_list = []

    for config in config_file_list:

        # Write application.properties, bootstrap.sh, and process_job.py to s3_app_folder
        s3_app_folder, s3_output_path = create_s3_dir.create(config)

        # If we don't have a cluster yet, start one
        if not cluster_id:
            # Start emr and wait until it's read
            cluster_id = emr.start(s3_app_folder)

        step_id = emr.add_step(cluster_id, s3_app_folder)

        for i in range(0, 1000):
            step_status = emr.monitor_step(cluster_id, step_id)

            if step_status == 'COMPLETED':
                output_list.append(s3_output_path)

                # Sending a second step immediately makes it hang on the second, apparently
                time.sleep(60)
                break

            elif step_status not in ["PENDING", "RUNNING"]:
                output_list.append(None)
                break

            else:
                time.sleep(60)

            print 'Step {0} has status {1}'.format(step_id, step_status)

    emr.terminate(cluster_id)

    return output_list


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Run point and polygon using AWS EMR')
    parser.add_argument('--config', '-c', nargs='*', help='config file of parameters', required=True)
    args = parser.parse_args()

    run(args.config)
