#!/bin/bash
for i in `seq 215 255`; do sudo ifconfig enp0s3:$i 192.168.1.$i up ; done
