import time
import click

from hadoop_pip.utilities import emr, create_s3_dir


@click.command()
@click.option('--instance_count', default=1, help='number of worker instances')
@click.option('--name', help='cluster name')
@click.argument('config_files', nargs=-1)
def cli(config_files, **kwargs):
    run(config_files, **kwargs)


def run(config_files, **kwargs):

    cluster_id = None
    all_steps_output_list = []

    for config in config_files:

        # # Write application.properties, bootstrap.sh, and process_job.py to s3_app_folder
        s3_app_folder, s3_output_list = create_s3_dir.create(config)

        # If we don't have a cluster yet, start one
        if not cluster_id:
            # Start emr and wait until it's read
            cluster_id = emr.start(s3_app_folder, **kwargs)

        step_id = emr.add_step(cluster_id, s3_app_folder)

        for i in range(0, 1000):
            step_status = emr.monitor_step(cluster_id, step_id)

            if step_status == 'COMPLETED':
                all_steps_output_list.append(s3_output_list)

                # Sending a second step immediately makes it hang on the second, apparently
                time.sleep(60)
                break

            elif step_status not in ["PENDING", "RUNNING"]:
                all_steps_output_list.append(None)
                break

            else:
                time.sleep(60)

            click.echo('Step {0} has status {1}'.format(step_id, step_status))

    emr.terminate(cluster_id)

    return all_steps_output_list


if __name__ == '__main__':
    cli()

