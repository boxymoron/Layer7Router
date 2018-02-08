#!/bin/bash
java -server -Xmx4G -XX:+AlwaysPreTouch -XX:MaxDirectMemorySize=8G -XX:+UseParallelGC -Djava.net.preferIPv4Stack=true -jar ../lib/Layer7Router.jar -backend_host 127.0.0.1 -backend_port 7180
