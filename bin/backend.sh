#!/bin/bash
java -cp ../lib/Layer7Router.jar -Xms1G -Xmx2G -XX:+AlwaysPreTouch -XX:MaxDirectMemorySize=8G -XX:+UseParallelGC -Djava.net.preferIPv4Stack=true Layer7RouterBackend -num_threads 4 -buffer_size 1024
