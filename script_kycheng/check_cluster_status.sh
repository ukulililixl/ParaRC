#!/bin/bash

for i in {1..18}; do echo "data node $i"; ssh dn$i 'ps aux | grep Dist | grep -v grep'; done

echo "coordinator"
ps aux | grep Dist | grep -v grep


