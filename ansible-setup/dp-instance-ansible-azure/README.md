 
 
 A ansible setup for data-platform-instance
 1. Node Exporter installation
 2. Path create and Volumes mount for Kafka


Variables in inventory/hosts
1. `hosts` IP address of instances where installation will done.
2. `mount_disks` option to mount volumes on instances.if set to true, It will create two path /data/kafka-data xfs file extension and mount to Azure Disks on instance. 
  
 
 #### Zookeeper
 Update IP address of instances in hosts and set mount_disks: false
 ```
 all:
    hosts:
        10.66.25.1:
        10.66.19.2:
        10.66.7.3:
    vars:
        mount_disks: false
 ```

 #### Schema Registry
 Update IP address of instances in hosts and set mount_disks: false
 ```
 all:
    hosts:
        10.66.25.1:
        10.66.19.2:
        10.66.7.3:
    vars:
        mount_disks: false
 ```

#### Kafka
Update IP address of instances in hosts and set mount_disks: true, setting true will create and mount path /data/kafka-data1 and /data/kafka-data2 to EBS. 
 
```
all:
    hosts:
        10.66.25.194:
        10.66.19.127:
        10.66.7.115:
    vars:
        mount_disks: true
        disks:
            - 1
            - 2
        mount_path: /mnt/data/kafka-data
 ```

#### Usage
```
ansible-playbook -i inventory/hosts playbooks/main.yml
```
