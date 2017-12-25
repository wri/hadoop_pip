# hadoop_pip
Manage AWS EMR clusters running Spark PIP

Helper script to send steps to EMR machines. Main usage:

```
usage: run_pip.py [-h] --config [CONFIG [CONFIG ...]]
run_pip.py: error: argument --config/-c is required
```

Pass a config (or list of configs) to `run_pip.py`. This will write application.properties files for each job to S3.

It will then start an EMR cluster, and passing each application.properties file to `process_job.py` to kick off each job.

If run within other python code (by importing function `run` from run_pip.py), can be used to return the CSV point-in-polygon outputs from these jobs.
