import argparse
import boto3
import os
import yaml
import re
import sys # For graceful exit
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

# --- Constants ---
# Hardcoded service tags
KAFKA_SERVICE_TAG = 'kafka'
ZOOKEEPER_SERVICE_TAG = 'zookeeper'

# Kafka disk configuration (assuming 2 disks per instance)
NUM_KAFKA_DISKS_PER_INSTANCE = 2
KAFKA_MOUNT_PATH = '/mnt/data/kafka-data'
KAFKA_DISK_SUFFIX = '1,2' # For hosts.ini "disks" variable, matches NUM_KAFKA_DISKS_PER_INSTANCE

# # Default Ansible user and interpreter for dp-instance-ansible
# ANSIBLE_DP_USER = 'ec2-user'
# ANSIBLE_DP_PYTHON_INTERPRETER = '/usr/bin/python3'

# cp-ansible specific configurations
CP_ANSIBLE_USER = 'ubuntu'
# CP_ANSIBLE_SSH_KEY_PATH = '~/.ssh/onlyDev.pem' # Consider overriding this in Jenkins
CP_ANSIBLE_SSH_COMMON_ARGS = '-o StrictHostKeyChecking=no'
CP_ANSIBLE_JMX_PORT = 7071
CP_ANSIBLE_HEAP_OPTS = "-Xms1g -Xmx4g"
CP_ANSIBLE_JMX_PORT_KAFKA = 9000


# --- Helper Functions (for common tasks) ---
def _create_directory_if_not_exists(path):
    """Helper to create directories, handling existing ones."""
    try:
        os.makedirs(path, exist_ok=True)
        print(f"\nEnsured directory exists: {path}")
    except OSError as e:
        print(f"\nError creating directory {path}: {e}")
        raise # Re-raise to stop execution if directory creation fails


# --- Core Logic Class ---
class ClusterInventoryGenerator:
    """
    Manages fetching EC2 instance details, generating Ansible inventory files,
    creating Route 53 DNS records, and tagging EC2 instances for Kafka and Zookeeper clusters.
    """

    def __init__(self, region, subservice, hosted_zone_id, base_ansible_setup_dir):
        """
        Initializes the generator with cluster-specific and AWS details.
        """
        self.region = region
        self.subservice = subservice
        self.hosted_zone_id = hosted_zone_id
        self.base_ansible_setup_dir = base_ansible_setup_dir

        self.ec2_client = boto3.client('ec2', region_name=self.region)
        self.route53_client = boto3.client('route53', region_name=self.region)

        print("Using Role: ", boto3.client('sts').get_caller_identity())

        self.kafka_instances = [] # Stores list of {'ip': ..., 'id': ..., 'name': ...}
        self.zookeeper_instances = [] # Stores list of {'ip': ..., 'id': ..., 'name': ...}

        # Paths for generated files
        self.dp_inventory_path = os.path.join(self.base_ansible_setup_dir, 'dp-instance-ansible', 'inventory', 'hosts.ini')
        self.cp_ansible_env_dir = os.path.join(self.base_ansible_setup_dir, 'cp-ansible', f"prod-{self.region}")
        self.cp_ansible_hosts_path = os.path.join(self.cp_ansible_env_dir, f"{self.subservice}-hosts.yml")


    def _fetch_instances_details(self, service_tag):
        """
        Fetches private IPs, Instance IDs, and Names for a given service type.
        Returns a sorted list of dictionaries, e.g., [{'ip': '...', 'id': '...', 'name': '...'}]
        """
        instances_details = []
        filters = [
            {'Name': 'instance-state-name', 'Values': ['running']},
            {'Name': 'tag:Service', 'Values': [service_tag]},
            {'Name': 'tag:SubService', 'Values': [self.subservice]}
        ]

        try:
            paginator = self.ec2_client.get_paginator('describe_instances')
            pages = paginator.paginate(Filters=filters)
            
            for page in pages:
                for reservation in page['Reservations']:
                    for instance in reservation['Instances']:
                        instance_name = next((tag['Value'] for tag in instance.get('Tags', []) if tag['Key'] == 'Name'), None)
                        instances_details.append({
                            'ip': instance['PrivateIpAddress'],
                            'id': instance['InstanceId'],
                            'name': instance_name
                        })
        except (ClientError, NoCredentialsError, PartialCredentialsError) as e:
            print(f"Error fetching EC2 instances for Service={service_tag}, SubService={self.subservice} in region {self.region}: {e}")
            return [] # Return empty list on error, allow other services to proceed
        except Exception as e:
            print(f"An unexpected error occurred while fetching EC2 instances: {e}")
            return []
        
        # Sort by IP address for consistent ordering
        return sorted(instances_details, key=lambda x: x['ip'])

    def fetch_all_cluster_details(self):
        """Fetches details for both Kafka and Zookeeper clusters."""
        print(f"--- Fetching instances for subservice: {self.subservice} in region: {self.region} ---")

        self.kafka_instances = self._fetch_instances_details(KAFKA_SERVICE_TAG)
        if not self.kafka_instances:
            print(f"Warning: No Kafka instances found with Service='{KAFKA_SERVICE_TAG}', SubService='{self.subservice}'.")
        else:
            print(f"Found Kafka: {len(self.kafka_instances)} instances.")

        self.zookeeper_instances = self._fetch_instances_details(ZOOKEEPER_SERVICE_TAG)
        if not self.zookeeper_instances:
            print(f"Warning: No Zookeeper instances found with Service='{ZOOKEEPER_SERVICE_TAG}', SubService='{self.subservice}'.")
        else:
            print(f"Found Zookeeper: {len(self.zookeeper_instances)} instances.")

        if not self.kafka_instances and not self.zookeeper_instances:
            print("Error: No instances found for either Kafka or Zookeeper. Exiting.")
            sys.exit(1) # Exit if no instances are found at all


    def generate_dp_ansible_inventory(self):
        """Generates the Ansible inventory/hosts.ini file for initial setup."""
        _create_directory_if_not_exists(os.path.dirname(self.dp_inventory_path))

        with open(self.dp_inventory_path, 'w') as f:
            f.write("[kafka]\n")
            if self.kafka_instances:
                f.write("".join([
                    f"{inst['ip']} mount_disks=true mount_path={KAFKA_MOUNT_PATH} disks={KAFKA_DISK_SUFFIX}\n"
                    for inst in self.kafka_instances
                ]))
            
            f.write("\n[zookeeper]\n")
            if self.zookeeper_instances:
                f.write("".join([f"{inst['ip']} mount_disks=false\n" for inst in self.zookeeper_instances]))
            
            # f.write(f"\n[all:vars]\n")
            # f.write(f"ansible_user={ANSIBLE_DP_USER}\n")
            # f.write(f"ansible_python_interpreter={ANSIBLE_DP_PYTHON_INTERPRETER}\n")

        print(f"Ansible dp-instance-ansible inventory generated at: {self.dp_inventory_path}")


    def generate_cp_ansible_hosts_file(self):
        """Generates the <subservice>-hosts.yml file for cp-ansible."""
        _create_directory_if_not_exists(self.cp_ansible_env_dir)

        zookeeper_connect_string = f"zookeeper-{self.subservice}.moeinternal.com:2181/kafka-{self.subservice}"
        kafka_log_dirs = [f"{KAFKA_MOUNT_PATH}{i}/topic-data" for i in range(1, NUM_KAFKA_DISKS_PER_INSTANCE + 1)]
        kafka_log_dirs_string = ",".join(kafka_log_dirs)

        cp_ansible_inventory_data = {
            'all': {
                'vars': {
                    'ansible_connection': 'ssh',
                    'ansible_user': CP_ANSIBLE_USER,
                    'ansible_become': True,
                    # 'ansible_ssh_private_key_file': CP_ANSIBLE_SSH_KEY_PATH,
                    'ansible_ssh_common_args': CP_ANSIBLE_SSH_COMMON_ARGS,
                    'jmxexporter_enabled': True,
                    'kafka_broker_jmxexporter_port': CP_ANSIBLE_JMX_PORT,
                    'zookeeper_jmxexporter_port': CP_ANSIBLE_JMX_PORT,
                    'schema_registry_jmxexporter_port': CP_ANSIBLE_JMX_PORT,
                    'confluent_server_enabled': False,
                    'zookeeper_custom_properties': {
                        'tickTime': 2000, 'initLimit': 5, 'syncLimit': 2, 'dataDir': '/var/zookeeper',
                        'clientPort': 2181, 'admin.serverPort': 8082, 'snapCount': 10000,
                        'autopurge.snapRetainCount': 10, 'autopurge.purgeInterval': 12, 'maxClientCnxns': 100
                    },
                    'kafka_broker_custom_properties': {
                        'zookeeper.connect': zookeeper_connect_string, 'zookeeper.session.timeout.ms': 30000,
                        'log.dirs': kafka_log_dirs_string, 'log.retention.hours': 24, 'log.roll.hours': 2,
                        'log.segment.bytes': 512000000, 'log.roll.jitter.ms': 300000,
                        'log.cleaner.delete.retention.ms': 3600000, 'compression.type': 'lz4',
                        'message.max.bytes': 10485760, 'default.replication.factor': 3,
                        'delete.topic.enable': False, 'auto.create.topics.enable': False,
                        'num.recovery.threads.per.data.dir': 2, 'num.replica.alter.log.dirs.threads': 1,
                        'num.partitions': 4, 'offsets.topic.num.partitions': 3,
                        'transaction.state.log.num.partitions': 3, 'fetch.max.bytes': 10485760,
                        'replica.fetch.max.bytes': 10485760, 'background.threads': 4,
                        'num.network.threads': 4, 'num.io.threads': 4, 'num.replica.fetchers': 4
                    },
                    'kafka_broker_service_environment_overrides': {
                        'KAFKA_HEAP_OPTS': CP_ANSIBLE_HEAP_OPTS, 'JMX_PORT': CP_ANSIBLE_JMX_PORT_KAFKA
                    }
                },
                'children': {
                    'zookeeper': {'hosts': {}},
                    'kafka_broker': {'hosts': {}}
                }
            }
        }

        # Populate Zookeeper hosts with IDs
        if self.zookeeper_instances:
            for i, inst in enumerate(self.zookeeper_instances):
                cp_ansible_inventory_data['all']['children']['zookeeper']['hosts'][inst['ip']] = {'zookeeper_id': i + 1}
        else:
            print("Warning: No Zookeeper IPs found for cp-ansible hosts file.")

        # Populate Kafka broker hosts with IDs
        if self.kafka_instances:
            for i, inst in enumerate(self.kafka_instances):
                cp_ansible_inventory_data['all']['children']['kafka_broker']['hosts'][inst['ip']] = {'broker_id': i + 1}
        else:
            print("Warning: No Kafka IPs found for cp-ansible hosts file.")

        try:
            with open(self.cp_ansible_hosts_path, 'w') as f:
                yaml.dump(cp_ansible_inventory_data, f, default_flow_style=False, sort_keys=False)
            print(f"cp-ansible hosts file generated at: {self.cp_ansible_hosts_path}")
        except IOError as e:
            print(f"Error writing cp-ansible hosts file {self.cp_ansible_hosts_path}: {e}")
            raise


    def create_route53_dns_records(self):
        """Creates A records in Route 53 for Kafka and Zookeeper IPs."""
        print("\n--- Creating Route 53 DNS Records ---")
        changes = []

        if self.kafka_instances:
            kafka_record_name = f"kafka-{self.subservice}.moeinternal.com"
            kafka_resource_records = [{'Value': inst['ip']} for inst in self.kafka_instances]
            changes.append({
                'Action': 'UPSERT', 'ResourceRecordSet': {
                    'Name': kafka_record_name, 'Type': 'A', 'TTL': 60, 'ResourceRecords': kafka_resource_records
                }
            })
            print(f"Prepared DNS record for Kafka: {kafka_record_name} -> {[inst['ip'] for inst in self.kafka_instances]}")

        if self.zookeeper_instances:
            zookeeper_record_name = f"zookeeper-{self.subservice}.moeinternal.com"
            zookeeper_resource_records = [{'Value': inst['ip']} for inst in self.zookeeper_instances]
            changes.append({
                'Action': 'UPSERT', 'ResourceRecordSet': {
                    'Name': zookeeper_record_name, 'Type': 'A', 'TTL': 60, 'ResourceRecords': zookeeper_resource_records
                }
            })
            print(f"Prepared DNS record for Zookeeper: {zookeeper_record_name} -> {[inst['ip'] for inst in self.zookeeper_instances]}")

        if not changes:
            print("No DNS records to create/update.")
            return

        try:
            response = self.route53_client.change_resource_record_sets(
                HostedZoneId=self.hosted_zone_id,
                ChangeBatch={'Changes': changes}
            )
            print(f"Route 53 DNS record creation initiated: {response['ChangeInfo']['Status']}")
            
            # Wait for propagation with timeout
            waiter = self.route53_client.get_waiter('resource_record_sets_changed')
            waiter.wait(Id=response['ChangeInfo']['Id'], WaiterConfig={'Delay': 10, 'MaxAttempts': 30}) # 5 minutes max
            print("Route 53 DNS records created/updated successfully and propagated.")
        except ClientError as e:
            print(f"Error creating/updating Route 53 DNS records: {e}")
            sys.exit(1) # Exit if DNS creation fails
        except Exception as e:
            print(f"An unexpected error occurred during DNS propagation wait: {e}")
            sys.exit(1)


    def tag_ec2_instances(self):
        """
        Adds NodeId (extracted from instance name) and prometheus tags to Kafka EC2 instances.
        Adds prometheus tag to Zookeeper EC2 instances.
        """
        print("\n--- Tagging EC2 Instances ---")

        # Tagging Kafka instances
        if self.kafka_instances:
            for inst in self.kafka_instances:
                node_id_value = None
                if inst['name']:
                    # Regex to extract the pattern like 'oneshot-2' from 'staging-dataplatform-kafka-oneshot-2'
                    # More robust regex to handle various prefixes, looking for the <subservice>-<number> suffix
                    match = re.search(rf'[-_]?{re.escape(self.subservice)}-(\d+)$', inst['name'])
                    if match:
                        node_id_value = f"{self.subservice}-{match.group(1)}"
                    else:
                        print(f"Warning: Could not extract subservice-number from instance name: {inst['name']} for {inst['id']}. Falling back to sequential ID.")
                        node_id_value = f"{self.subservice}-fallback-{self.kafka_instances.index(inst) + 1}"
                else:
                    print(f"Warning: Instance name not found for {inst['id']}. Falling back to sequential ID.")
                    node_id_value = f"{self.subservice}-fallback-{self.kafka_instances.index(inst) + 1}"

                if node_id_value:
                    try:
                        self.ec2_client.create_tags(
                            Resources=[inst['id']],
                            Tags=[
                                {'Key': 'NodeId', 'Value': node_id_value},
                                {'Key': 'prometheus', 'Value': 'enabled'}
                            ]
                        )
                        print(f"Tagged Kafka instance {inst['id']} (Name: {inst['name'] or 'N/A'}) with NodeId: {node_id_value} and prometheus:enabled.")
                    except ClientError as e:
                        print(f"Error tagging Kafka instance {inst['id']}: {e}")
                else:
                    print(f"Skipping tagging for Kafka instance {inst['id']} (Name: {inst['name'] or 'N/A'}) due to missing NodeId value.")
        else:
            print("No Kafka instances to tag for NodeId and Prometheus.")

        # Tagging Zookeeper instances with 'prometheus: enabled'
        if self.zookeeper_instances:
            for inst in self.zookeeper_instances:
                try:
                    self.ec2_client.create_tags(
                        Resources=[inst['id']],
                        Tags=[{'Key': 'prometheus', 'Value': 'enabled'}]
                    )
                    print(f"Tagged Zookeeper instance {inst['id']} (Name: {inst['name'] or 'N/A'}) with prometheus:enabled.")
                except ClientError as e:
                    print(f"Error tagging Zookeeper instance {inst['id']}: {e}")
        else:
            print("No Zookeeper instances to tag for Prometheus.")
        
        print("EC2 instance tagging process completed.")


    def run(self):
        """Orchestrates the entire process."""
        try:
            self.fetch_all_cluster_details()
            self.generate_dp_ansible_inventory()
            self.generate_cp_ansible_hosts_file()
            # self.create_route53_dns_records()
            self.tag_ec2_instances()
            print("\nAutomation completed successfully!")
        except Exception as e:
            print(f"\nFATAL ERROR during automation: {e}")
            sys.exit(1)


# --- Main Execution Block ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Automate EC2 IP fetching, Ansible inventory generation, DNS creation, and EC2 tagging for Kafka/Zookeeper clusters.")
    parser.add_argument("--region", required=True, help="AWS region (e.g., ap-south-1)")
    parser.add_argument("--subservice", required=True, help="Value for 'SubService' tag for both Kafka and Zookeeper instances (e.g., your-cluster-name)")
    parser.add_argument("--hosted-zone-id", required=True, help="Route 53 Hosted Zone ID for moeinternal.com (e.g., ZXXXXXXXXXXXXX)")

    args = parser.parse_args()

    # Calculate base_ansible_setup_dir relative to the script's location
    # This assumes the script is in 'ansible-setup/scripts/'
    # and the base is 'ansible-setup/'
    BASE_ANSIBLE_SETUP_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    print(f"Base Ansible setup directory: {BASE_ANSIBLE_SETUP_DIR}\n")

    generator = ClusterInventoryGenerator(
        region=args.region,
        subservice=args.subservice,
        hosted_zone_id=args.hosted_zone_id,
        base_ansible_setup_dir=BASE_ANSIBLE_SETUP_DIR
    )

    generator.run()