#!/bin/bash

# install wget is not exist on system
#sudo apt-get update || true
#sudo apt-get install wget -y
sudo apt install jq -y

# Download and place confd at /opt/confd
cd /tmp/
arch=""
uname -a | grep aarch64
if [ "$?" -ne 0 ]
then
    arch="amd64"
    export LC_ALL="en_US.UTF-8"
    sudo apt install python-pip
else
    arch="arm64"
    sudo rm -f /usr/local/bin/python
    sudo ln -s /usr/bin/python3 /usr/bin/python
fi

wget https://github.com/kelseyhightower/confd/releases/download/v0.16.0/confd-0.16.0-linux-$arch

sudo mkdir -p /opt/confd/bin

sudo mv confd-0.16.0-linux-$arch /opt/confd/bin/confd

sudo chmod +x /opt/confd/bin/confd
sudo rm confd-0.16.0-linux-$arch || true

# set path to bashrc
echo export PATH="\$PATH:/opt/confd/bin/" | sudo tee -a ~/.bashrc

#export LC_ALL="en_US.UTF-8"

# install schedule which is consumed to run task in regular interval
sudo apt-get -y install python3-pip
sudo apt  install awscli -y
sudo pip3 install schedule
sudo pip3 install pyyaml==3.13


# Create al file for confd scheduler logging
sudo touch /var/log/confd.log


exit 0