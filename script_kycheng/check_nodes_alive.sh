#!/bin/bash

for i in {1..18}; do echo "data node $i"; ping -c3 dn$i; done


