#!/bin/bash

# Prerequisite: Please run install_confd.sh before running this script.

#########################################################################################
#########################################################################################
### Script to configure confd on a ubuntu 16.04 system                                ###
#########################################################################################
#########################################################################################

# Create directories to configure intial confd
sudo mkdir -p /etc/confd
sudo mkdir -p /etc/confd/conf.d
sudo mkdir -p /etc/confd/templates
sudo mkdir -p /etc/confd/workdir
sudo mkdir -p /etc/confd/workdir/configs


uname -a | grep aarch64
if [ "$?" -ne 0 ]
then
    #Systemd file for amd64
    sudo tee /etc/systemd/system/confd.service << EOF
    [Unit]
    Description="CASCAD"
    Documentation=https://www.consul.io/
    Requires=network-online.target
    After=network-online.target

    [Service]
    User=root
    Group=root
    ExecStart=/bin/bash -c 'python /etc/confd/workdir/jobs.py'
    Restart=always
    KillMode=process
    LimitNOFILE=65536

    [Install]
    WantedBy=multi-user.target
EOF

else
    #Systemd file for aarch64 / arm64
    sudo tee /etc/systemd/system/confd.service << EOF
    [Unit]
    Description="CASCAD"
    Documentation=https://www.consul.io/
    Requires=network-online.target
    After=network-online.target

    [Service]
    User=root
    Group=root
    ExecStart=/bin/bash -c 'python3 /etc/confd/workdir/jobs.py'
    Restart=always
    KillMode=process
    LimitNOFILE=65536

    [Install]
    WantedBy=multi-user.target
EOF
fi


# Create top toml
sudo tee /etc/confd/conf.d/top.toml << EOF
[template]
src = "generic.tmpl"
dest = "/etc/confd/workdir/top.yml"
mode = "0644"
keys = [
  "top.yml"
  ]
prefix = "/confd-configs/templates/612427630422/us-east-1"
reload_cmd = "sudo python3 /etc/confd/workdir/configurator.py"
check_cmd = "ls /etc/confd/templates/"
EOF

# Generic template consumed by all toml file
sudo tee /etc/confd/templates/generic.tmpl << EOF
{{range gets "/*"}}{{.Value}}{{end}}
EOF

# Copy configurator file to sync all files from consul
sudo tee /etc/confd/workdir/configurator.py << EOF
#!/usr/bin/env python
import json
import os
import shelve
import yaml


LOCAL = False
if not LOCAL:
    import requests
    import shutil
    import os.path
    import subprocess

    new_env_file = "/tmp/confd_env.json"
    with open(new_env_file, "w") as f:
        try:
            instance_identity = json.loads(requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document").content)
            instance_id = instance_identity['instanceId']
            region = instance_identity['region']
            resp = subprocess.Popen(["aws", "ec2", "describe-tags", "--region", region, "--filters", "Name=resource-id,Values=%s" % instance_id], stdout=subprocess.PIPE).communicate()[0] 
            json_out = json.loads(resp)
            jsonresp = {"tags":{}}
            for item in json_out["Tags"]:
                jsonresp["tags"][item["Key"]] = item["Value"]
            if not ("FabTag" in jsonresp["tags"].keys()):
                jsonresp["tags"]["FabTag"] = "None"
            lines = [jsonresp]
            f.write(json.dumps(lines))
            print (lines)
        except ValueError:
            print ("Error fetching from AWS.. trying to fetch from melora")
            resp = requests.get("http://melora.moengage.com/whoami").content
            jsonresp = json.loads(resp)
            if not "tags" in jsonresp :
                jsonresp["tags"] = {}
            if not ("tags" in jsonresp and "FabTag" in jsonresp["tags"]):
                jsonresp["tags"]["FabTag"] = "None"
            lines = [jsonresp]
            f.write(json.dumps(lines))
            print (lines)


CONFD_CONFIG_DIR = "/etc/confd/conf.d/"
CONFD_TEMPLATE_DIR = "/etc/confd/templates/"
CONFIGURATOR_WORK_DIR = "/etc/confd/workdir/"
CONFIG_TEMPLATE = """[template]
src = '{src}'
dest = '/etc/confd/conf.d/orig{filename}.toml'
mode = '0644'
keys = ['{key}']
prefix = '{prefix}'
reload_cmd = "{reload_cmd}"
"""
GENERIC_TEMPLATE_FILENAME = 'generic.tmpl'
GENERIC_TEMPLATE_FILEPATH = CONFD_TEMPLATE_DIR + GENERIC_TEMPLATE_FILENAME
GENERIC_TEMPLATE = """{{range gets "/*"}}{{.Value}}{{end}}"""
TOP_FILENAME = "top.yml"
TOP_FILEPATH = CONFIGURATOR_WORK_DIR + TOP_FILENAME
INTERMEDIATE_FILENAME = 'intermediate{filename}.toml'
INTERMEDIATE_FILEPATH = CONFD_CONFIG_DIR + INTERMEDIATE_FILENAME
RELOAD_CMD = 'sudo python3 /etc/confd/workdir/configurator.py'
STORAGE_FILEPATH = CONFIGURATOR_WORK_DIR + 'storage.json'
CONFD_ENV_JSON_FILEPATH = "/tmp/confd_env.json"


class ConfiguratorStorage(object):
    def __init__(self):
        self.storage_file = STORAGE_FILEPATH

    def __enter__(self):
        self.storage = shelve.open(self.storage_file)
        return self.storage

    def __exit__(self, *args):
        self.storage.close()
        return False


def main():
    if not os.path.isfile(TOP_FILEPATH):
        with open(TOP_FILEPATH, 'w') as fp:
            pass

    with open(CONFD_ENV_JSON_FILEPATH, 'r') as fp:
        envjson = json.load(fp)
        business = ''
        fab_tag = ''
        for item in envjson:
            tags = item.get('tags', {})
            business = tags.get('Business', '')
            fab_tag = tags.get('FabTag', '')


    with open(TOP_FILEPATH, 'r') as fp:
        content = yaml.load(fp)
        templates = content.get('commons', [])
        all_fabtag_per_business = content.get(business, [])
        for tag in all_fabtag_per_business:
            templates.extend(tag.get(fab_tag, []))
        for line in templates:
            key = line.strip()
            filename = key.replace('/', '-').replace('.', '-')
            if filename:
                FILEPATH = INTERMEDIATE_FILEPATH.format(filename=filename)
                prefix, key = key.rsplit('/', 1)
                if not os.path.exists(FILEPATH):
                    with open(FILEPATH, 'w') as fp:
                        fp.write(
                            CONFIG_TEMPLATE.format(
                                src=GENERIC_TEMPLATE_FILENAME,
                                filename=filename,
                                prefix=prefix,
                                key=key,
                                reload_cmd=RELOAD_CMD
                            )
                        )

    if not os.path.exists(GENERIC_TEMPLATE_FILEPATH):
        with open(GENERIC_TEMPLATE_FILEPATH, 'w') as fp:
            fp.write(GENERIC_TEMPLATE)

if __name__ == '__main__':
    main()
EOF


# Copy jobs file to run consul in every 60 sec
sudo tee /etc/confd/workdir/jobs.py << EOF
#!/usr/bin/python
import os
import schedule
import time
import logging  # ensures that logging module is configured

logging.basicConfig(filename='/var/log/confd.log',
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)

logger = logging.getLogger('jobs')

def log_job(job_func):
    logger.info("Running {}".format(job_func.__name__))
    job_func()


def trigger_confd():
    os.system('sudo find /etc/confd/conf.d/ -size 1c -type f -delete')
    os.system('sudo /opt/confd/bin/confd -backend consul -node consul.moengage.com:443 -scheme https -onetime')
    logger.info("Confd job is started!!")

schedule.every(60).seconds.do(log_job, trigger_confd)


def main():
    logger.info('Starting schedule loop')
    while True:
        schedule.run_pending()
        time.sleep(5)


if __name__ == '__main__':
    main()

EOF


# Create a top file, since it is consumed by configurator
sudo touch /etc/confd/workdir/top.yml

# Change owner to ubuntu and add execution mode to the file
sudo chown ubuntu:ubuntu -R /etc/confd
sudo chmod +x /etc/confd/workdir/configurator.py /etc/confd/workdir/jobs.py

# Run Confd to sync all files
sudo /opt/confd/bin/confd -backend consul -node consul.moengage.com:443 -scheme https -onetime -log-level debug

# Run configurator to generate all toml files
sudo python3 /etc/confd/workdir/configurator.py

# Restart the confd scheduler
#sudo service confd restart
#sudo systemctl enable confd
#sudo systemctl start confd
sudo systemctl enable confd
exit 0