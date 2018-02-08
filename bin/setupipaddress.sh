#!/bin/bash

INTERFACE_NAME="enp0s3"
BASE_IP=192.168.1
START_OCTET=215
END_OCTET=255

for i in `seq $START_OCTET $END_OCTET`; do 
	sudo ifconfig $INTERFACE_NAME:$i $BASE_IP.$i up ; 
done
