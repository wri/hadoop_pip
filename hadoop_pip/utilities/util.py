import json
import subprocess


def run_subprocess(cmd):

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    subprocess_list = []

    # Read from STDOUT
    for line in iter(p.stdout.readline, b''):
        subprocess_list.append(line.strip())

    print '\n'.join(subprocess_list)

    return subprocess_list


def response_to_dict(subprocess_list):

    print 'Subprocess response: ' + str(subprocess_list)
    print '***************'

    to_dict = json.loads(''.join(subprocess_list))

    return to_dict
