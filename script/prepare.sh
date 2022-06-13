#!/bin/bash

# configure sudo without passwd
# sudo visudo
# lixl ALL=(ALL) NOPASSWD:ALL

# configure hostname
# 172.16.108.138 node0
# 172.16.108.133 node1
# 172.16.108.135 node2
# 172.16.108.136 node3
# 172.16.108.137 node4

# configure ssh without passwd
ssh-keygen
for i in 0 1 2 3 4
do
  ssh-copy-id lixl@node$i
done

sudo apt-get install gcc g++ make autoconf libtool help2man -y

# install cmake 3.10.1
# ./bootstrap & make & sudo make install

# install hiredis
# make & sudo make install

# install redis-3.2.8
# make & sudo make install
# cd util; sudo ./install_server.sh

# gf-complete
# ./configure; make; sudo make install;

# install nasm
# ./configure; make; sudo make install;

# install isa-l
# ./configure; make; sudo make install;

# comment .bashrc line 5-9
# export
#export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
