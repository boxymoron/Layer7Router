To run frontend.sh and router.sh with more than 32K-ish concurrent connections:

	We need to overcome the limit of ports per ip-address on Linux. By default most Linux distros limit 
the number of port to 32K or so. By creating multiple virtual interfaces, each with its own ip-address,
we can have up to 32K * (number of interfaces) open TCP connections.

- Increase TCP stack default limits and others:
	- see 98-network-tuning.conf and limits.conf in ../conf/ directory

- Configure setupipaddress.sh
	- INTERFACE_NAME="enp0s3"
		- use "ifconfig -a" to find out your interface name
	- BASE_IP=192.168.1
		- check your LAN/router settings
	- START_OCTET=215
		- check your LAN/router settings
	- END_OCTET=255
		- check your LAN/router settings
	
... So we have a total of: (65000 - 13000) * (255 - 215) = 2080000 TCP sockets/connections avaialable to the system. 
Keep in mind if you are running router.sh then each incoming connection creates an additional TCP socket to the backend server,
so you will only be able to route/proxy/load-balance 2080000/2 - (system overhead) concurrent connections.

- Configure router.sh flags:
	- -client_base_ip 192.168.1
		- use same value as BASE_IP above
	- -client_start_ip 215 
		- use same value as START_OCTET above
	- -client_end_ip 255
		- use same value as END_OCTET above