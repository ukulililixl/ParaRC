#!/bin/bash

my_ip=$(ifconfig | grep 192.168.10 | sed "s/ *inet addr:\([^ ]*\).*/\1/")

iperf -s &

for i in {1..18}; do ssh dn${i} iperf -c ${my_ip}; done

killall iperf

