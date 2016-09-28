import json
import subprocess
from ConfigParser import SafeConfigParser


def read_config(config):

    parser = SafeConfigParser()
    parser.read(config)

    config_dict = {}

    for section in parser.sections():
        config_dict[section] = {}

        for option in parser.options(section):
            config_dict[section][option] = parser.get(section, option)

    return


def run_subprocess(cmd):

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    subprocess_list = []

    # Read from STDOUT
    for line in iter(p.stdout.readline, b''):
        subprocess_list.append(line.strip())

    print '\n'.join(subprocess_list)

    return subprocess_list


def response_to_dict(subprocess_list):

    to_dict = json.load(''.join(subprocess_list))

    return to_dict
