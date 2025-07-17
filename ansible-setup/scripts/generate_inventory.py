import argparse
import boto3
import os
import yaml
from botocore.exceptions import ClientError # Import for error handling

def fetch_ec2_ips(region, service, subservice):
    """
    Fetches private IPs of EC2 instances based on provided tags.
    Assumes instances are tagged with 'Service' and 'SubService'.
    Returns a tuple: (list of IPs, list of instance IDs)
    """
    ec2 = boto3.client('ec2', region_name=region)
    ips = []
    instance_ids = []
    instance_names = []

    filters = [
        {'Name': 'instance-state-name', 'Values': ['running']},
        {'Name': f'tag:Service', 'Values': [service]},
        {'Name': f'tag:SubService', 'Values': [subservice]}
    ]

    try:
        response = ec2.describe_instances(Filters=filters)
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                ips.append(instance['PrivateIpAddress'])
                instance_ids.append(instance['InstanceId'])
                # Optionally, fetch instance name if available
                for tag in instance.get('Tags', []):
                    if tag['Key'] == 'Name':
                        instance_names.append(tag['Value'])
    except Exception as e:
        print(f"Error fetching EC2 instances for Service={service}, SubService={subservice} in region {region}: {e}")
    
    return ips, instance_ids, instance_names

    # # Sort both lists based on IPs to ensure consistent ID assignment later
    # # Create a list of tuples (ip, instance_id)
    # combined_list = sorted(zip(ips, instance_ids))
    
    # # Unzip them back into two sorted lists
    # sorted_ips = [item[0] for item in combined_list]
    # sorted_instance_ids = [item[1] for item in combined_list]

    # return sorted_ips, sorted_instance_ids


def generate_ansible_inventory(kafka_ips, zookeeper_ips, output_dir):
    """
    Generates the Ansible inventory/hosts.ini file.
    """
    inventory_path = os.path.join(output_dir, 'inventory', 'hosts.ini')
    
    # Ensure the inventory directory exists
    os.makedirs(os.path.dirname(inventory_path), exist_ok=True)

    with open(inventory_path, 'w') as f:
        f.write("[kafka]\n")
        if kafka_ips:
            # Hardcoded disks to [1, 2] as per your latest provided code for generate_ansible_inventory
            f.write("".join([f"{ip} mount_disks=true mount_path=/mnt/data/kafka-data disks=1,2\n" for ip in kafka_ips]))
        else:
            print("Warning: No Kafka IPs found. Kafka cluster group will be empty in inventory.")
        
        f.write("\n[zookeeper]\n")
        if zookeeper_ips:
            f.write("".join([f"{ip} mount_disks=false\n" for ip in zookeeper_ips]))
        else:
            print("Warning: No Zookeeper IPs found. Zookeeper cluster group will be empty in inventory.")
            
        # These vars are commented out in your latest snippet for generate_ansible_inventory
        # f.write("\n[all:vars]\n")
        # f.write("ansible_user=ec2-user\n")
        # f.write("ansible_python_interpreter=/usr/bin/python3\n")

    print(f"Ansible inventory generated at: {inventory_path}")


def generate_cp_ansible_hosts_file(region, subservice, kafka_ips, zookeeper_ips, base_output_dir):
    """
    Generates the <subservice>-hosts.yml file for cp-ansible.
    """
    # Define the output directory for cp-ansible specific inventory
    cp_ansible_env_dir = os.path.join(base_output_dir, 'cp-ansible', 'staging', region)
    os.makedirs(cp_ansible_env_dir, exist_ok=True)

    cp_ansible_hosts_path = os.path.join(cp_ansible_env_dir, f"{subservice}-hosts.yml")

    # Construct the zookeeper.connect string dynamically
    zookeeper_connect_string = f"zookeeper-{subservice}.moeinternal.com:2181/kafka-{subservice}"

    # Construct log.dirs for Kafka dynamically
    # Assuming 2 disks per Kafka instance based on your template
    num_kafka_disks_per_instance = 2 
    kafka_log_dirs = [f"/mnt/data/kafka-data{i}/topic-data" for i in range(1, num_kafka_disks_per_instance + 1)]
    kafka_log_dirs_string = ",".join(kafka_log_dirs)


    # Build the YAML structure for cp-ansible hosts file
    cp_ansible_inventory = {
        'all': {
            'vars': {
                'ansible_connection': 'ssh',
                'ansible_user': 'ubuntu',
                'ansible_become': True,
                'ansible_ssh_private_key_file': '~/.ssh/onlyDev.pem',
                'ansible_ssh_common_args': '-o StrictHostKeyChecking=no',
                'jmxexporter_enabled': True,
                'kafka_broker_jmxexporter_port': 7071,
                'zookeeper_jmxexporter_port': 7071,
                'schema_registry_jmxexporter_port': 7071,
                'confluent_server_enabled': False,
                'zookeeper_custom_properties': {
                    'tickTime': 2000,
                    'initLimit': 5,
                    'syncLimit': 2,
                    'dataDir': '/var/zookeeper',
                    'clientPort': 2181,
                    'admin.serverPort': 8082,
                    'snapCount': 10000,
                    'autopurge.snapRetainCount': 10,
                    'autopurge.purgeInterval': 12,
                    'maxClientCnxns': 100
                },
                'kafka_broker_custom_properties': {
                    'zookeeper.connect': zookeeper_connect_string,
                    'zookeeper.session.timeout.ms': 30000,
                    'log.dirs': kafka_log_dirs_string,
                    'log.retention.hours': 24,
                    'log.roll.hours': 2,
                    'log.segment.bytes': 512000000,
                    'log.roll.jitter.ms': 300000,
                    'log.cleaner.delete.retention.ms': 3600000,
                    'compression.type': 'lz4',
                    'message.max.bytes': 10485760,
                    'default.replication.factor': 3,
                    'delete.topic.enable': False,
                    'auto.create.topics.enable': False,
                    'num.recovery.threads.per.data.dir': 2,
                    'num.replica.alter.log.dirs.threads': 1,
                    'num.partitions': 4,
                    'offsets.topic.num.partitions': 3,
                    'transaction.state.log.num.partitions': 3,
                    'fetch.max.bytes': 10485760,
                    'replica.fetch.max.bytes': 10485760,
                    'background.threads': 4,
                    'num.network.threads': 4,
                    'num.io.threads': 4,
                    'num.replica.fetchers': 4
                },
                'kafka_broker_service_environment_overrides': {
                    'KAFKA_HEAP_OPTS': "-Xms1g -Xmx4g",
                    'JMX_PORT': 9000
                }
            },
        },
        'zookeeper': {
            'hosts': {}
        },
        'kafka_broker': {
            'hosts': {}
        }
    }

    # Populate Zookeeper hosts with IDs
    if zookeeper_ips:
        for i, ip in enumerate(zookeeper_ips):
            cp_ansible_inventory['zookeeper']['hosts'][ip] = {'zookeeper_id': i + 1}
    else:
        print("Warning: No Zookeeper IPs found for cp-ansible hosts file.")

    # Populate Kafka broker hosts with IDs
    if kafka_ips:
        for i, ip in enumerate(kafka_ips):
            cp_ansible_inventory['kafka_broker']['hosts'][ip] = {'broker_id': i + 1}
    else:
        print("Warning: No Kafka IPs found for cp-ansible hosts file.")

    # Write the YAML to file
    with open(cp_ansible_hosts_path, 'w') as f:
        yaml.dump(cp_ansible_inventory, f, default_flow_style=False, sort_keys=False)

    print(f"cp-ansible hosts file generated at: {cp_ansible_hosts_path}")


def create_route53_dns_records(region, subservice, kafka_ips, zookeeper_ips, hosted_zone_id):
    """
    Creates A records in Route 53 for Kafka and Zookeeper IPs.
    """
    route53_client = boto3.client('route53', region_name=region)

    changes = []

    if kafka_ips:
        kafka_record_name = f"kafka-{subservice}.moeinternal.com"
        kafka_resource_records = [{'Value': ip} for ip in kafka_ips]
        changes.append({
            'Action': 'UPSERT',
            'ResourceRecordSet': {
                'Name': kafka_record_name,
                'Type': 'A',
                'TTL': 60,
                'ResourceRecords': kafka_resource_records
            }
        })
        print(f"Prepared DNS record for Kafka: {kafka_record_name} -> {kafka_ips}")
    else:
        print("No Kafka IPs provided for DNS record creation.")

    if zookeeper_ips:
        zookeeper_record_name = f"zookeeper-{subservice}.moeinternal.com"
        zookeeper_resource_records = [{'Value': ip} for ip in zookeeper_ips]
        changes.append({
            'Action': 'UPSERT',
            'ResourceRecordSet': {
                'Name': zookeeper_record_name,
                'Type': 'A',
                'TTL': 60,
                'ResourceRecords': zookeeper_resource_records
            }
        })
        print(f"Prepared DNS record for Zookeeper: {zookeeper_record_name} -> {zookeeper_ips}")
    else:
        print("No Zookeeper IPs provided for DNS record creation.")

    if not changes:
        print("No DNS records to create/update.")
        return

    try:
        response = route53_client.change_resource_record_sets(
            HostedZoneId=hosted_zone_id,
            ChangeBatch={'Changes': changes}
        )
        print(f"Route 53 DNS record creation initiated: {response['ChangeInfo']['Status']}")
        # wait till the change is propagated
        waiter = route53_client.get_waiter('resource_record_sets_changed')
        waiter.wait(Id=response['ChangeInfo']['Id'])
        print("Route 53 DNS records created/updated successfully.")
        # Return the ChangeInfo for further processing if needed
        return response['ChangeInfo']
    except ClientError as e:
        print(f"Error creating/updating Route 53 DNS records: {e}")
        exit(1) # Exit if DNS creation fails

def tag_ec2_instances_with_nodeid(region, subservice, kafka_instance_ids, kafka_instance_names):
    """
    Adds NodeId and prometheus tags to Kafka EC2 instances.
    Adds prometheus tag to Zookeeper EC2 instances.
    """
    ec2_client = boto3.client('ec2', region_name=region)
    
    if kafka_instance_ids:
        for i, instance_id in enumerate(kafka_instance_ids):
            # extract oneshot-1 from staging-dataplatform-kafka-oneshot-1
            if kafka_instance_names and i < len(kafka_instance_names):
                instance_name = kafka_instance_names[i]
                split_name = instance_name.split('-')
                if len(split_name) > 4:
                    nodeId = f"{split_name[3]}-{split_name[4]}"
                    try:
                        ec2_client.create_tags(Resources=[instance_id], Tags=[
                            {'Key': 'NodeId', 'Value': f"{nodeId}"},
                            {'Key': 'prometheus', 'Value': 'enabled'}
                        ])
                        print(f"Tagged Kafka instances with NodeId and prometheus tags.")
                    except ClientError as e:
                        print(f"Error tagging Kafka instances: {e}")
                        # Do not exit here, allow other operations to proceed if tagging partially fails
                else:
                    print(f"Instance name '{instance_name}' does not have enough parts to extract NodeId.")
    else:
        print("No Kafka instance IDs provided for NodeId tagging.")

    # Fetch Zookeeper instance IDs to tag them with 'prometheus: enabled'
    # Re-fetch as we only passed kafka_instance_ids to this function,
    # or modify the calling code to pass zookeeper_instance_ids as well.
    # For simplicity, let's re-fetch here, assuming you only need the prometheus tag
    # for Zookeeper and not a sequential NodeId based on this script's context.
    # If Zookeeper also needs sequential NodeId, adjust fetch_ec2_ips and parameter passing.
    zookeeper_ips, zookeeper_instance_ids, zookeeper_instance_names = fetch_ec2_ips(region, 'zookeeper', subservice)

    if zookeeper_instance_ids:
        prometheus_tags = []
        for instance_id in zookeeper_instance_ids:
            prometheus_tags.append({
                'ResourceId': instance_id,
                'Tags': [{'Key': 'prometheus', 'Value': 'enabled'}]
            })
        try:
            ec2_client.create_tags(DryRun=False, Resources=[id_tag['ResourceId'] for id_tag in prometheus_tags], Tags=[tag for id_tag in prometheus_tags for tag in id_tag['Tags']])
            print(f"Tagged Zookeeper instances with prometheus tag.")
        except ClientError as e:
            print(f"Error tagging Zookeeper instances: {e}")
    else:
        print("No Zookeeper instance IDs found to tag for Prometheus.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch EC2 IPs and generate Ansible inventory.")
    parser.add_argument("--region", required=True, help="AWS region (e.g., ap-south-1)")
    parser.add_argument("--subservice", required=True, help="Value for 'SubService' tag for both Kafka and Zookeeper instances (e.g., your-cluster-name)")
    parser.add_argument("--hosted-zone-id", required=True, help="Route 53 Hosted Zone ID for moeinternal.com (e.g., ZXXXXXXXXXXXXX)")

    args = parser.parse_args()

    KAFKA_SERVICE_TAG = 'kafka'
    ZOOKEEPER_SERVICE_TAG = 'zookeeper'
    
    BASE_ANSIBLE_SETUP_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    print(f"Fetching instances for subservice: {args.subservice} in region: {args.region}")
    print(f"Base Ansible setup directory: {BASE_ANSIBLE_SETUP_DIR}")

    # Fetch Kafka IPs and Instance IDs
    kafka_ips, kafka_instance_ids, kafka_instance_names = fetch_ec2_ips(args.region, KAFKA_SERVICE_TAG, args.subservice)
    if not kafka_ips:
        print(f"No Kafka instances found with Service='{KAFKA_SERVICE_TAG}', SubService='{args.subservice}'.")
    else:
        print(f"Found Kafka IPs: {kafka_ips}, Instance IDs: {kafka_instance_ids}, Instance Names: {kafka_instance_names}")
    
    # Fetch Zookeeper IPs and Instance IDs
    zookeeper_ips, zookeeper_instance_ids, zookeeper_instance_names = fetch_ec2_ips(args.region, ZOOKEEPER_SERVICE_TAG, args.subservice)
    if not zookeeper_ips:
        print(f"No Zookeeper instances found with Service='{ZOOKEEPER_SERVICE_TAG}', SubService='{args.subservice}'.")
    else:
        print(f"Found Zookeeper IPs: {zookeeper_ips}, Instance IDs: {zookeeper_instance_ids}, Instance Names: {zookeeper_instance_names}")

    if not kafka_ips and not zookeeper_ips:
        print("Error: No instances found for either Kafka or Zookeeper. Exiting.")
        exit(1)

    # Generate initial Ansible inventory (hosts.ini)
    generate_ansible_inventory(kafka_ips, zookeeper_ips, BASE_ANSIBLE_SETUP_DIR)

    # Generate cp-ansible specific hosts file
    generate_cp_ansible_hosts_file(args.region, args.subservice, kafka_ips, zookeeper_ips, BASE_ANSIBLE_SETUP_DIR)

    # Create DNS records in Route 53
    print("\n--- Creating Route 53 DNS Records ---")
    create_route53_dns_records(args.region, args.subservice, kafka_ips, zookeeper_ips, args.hosted_zone_id)

    # Tag EC2 instances
    print("\n--- Tagging EC2 Instances ---")
    tag_ec2_instances_with_nodeid(args.region, args.subservice, kafka_instance_ids, kafka_instance_names)
    # Zookeeper instances are also tagged for prometheus within the tag_ec2_instances_with_nodeid function.
    print("EC2 instance tagging process completed.")
