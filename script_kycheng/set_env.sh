#/bin/sh
rm ~/.wgetrc
echo "http_proxy=http://proxy.cse.cuhk.edu.hk:8000/
https_proxy=https://proxy.cse.cuhk.edu.hk:8000/
ftp_proxy=ftp://proxy.cse.cuhk.edu.hk:8000/
use_proxy=on" >> ~/.wgetrc

rm -rf ~/Tools
mkdir -p ~/Tools

cd ~/Tools/
wget http://ftp.gnu.org/gnu/automake/automake-1.14.1.tar.gz
tar -zxvf automake-1.14.1.tar.gz
cd automake-1.14.1/
./configure
make
sudo make install
automake --version
echo "export LC_ALL=C" >> ~/.bashrc

cd ~/Tools/
wget https://storage.googleapis.com/google-code-archive-downloads/v2/code.google.com/protobuf/protobuf-2.5.0.tar.gz
tar -zxvf protobuf-2.5.0.tar.gz
cd protobuf-2.5.0/
./configure
make
sudo make install

cd ~/Tools/
wget http://adslab.cse.cuhk.edu.hk/software/openec/redis-3.2.8.tar.gz
tar -zxvf redis-3.2.8.tar.gz
cd redis-3.2.8/
make
sudo make install
cd utils/
echo | sudo ./install_server.sh





sudo sed -i "s/127.0.0.1/0.0.0.0/g" /etc/redis/6379.conf
sudo /etc/init.d/redis_6379 stop
sudo /etc/init.d/redis_6379 start

cd ~/Tools/
wget http://adslab.cse.cuhk.edu.hk/software/openec/gf-complete.tar.gz
tar -zxvf gf-complete.tar.gz
cd gf-complete/
./configure
make
sudo make install

cd ~/Tools/
wget http://adslab.cse.cuhk.edu.hk/software/openec/hiredis.tar.gz
tar -zxvf hiredis.tar.gz
cd hiredis/
make
sudo make install

cd ~/Tools/
wget http://adslab.cse.cuhk.edu.hk/software/openec/isa-l-2.14.0.tar.gz
tar -zxvf isa-l-2.14.0.tar.gz
cd isa-l-2.14.0/
./configure
make
sudo make install

cd ~/
rm -rf apache-maven-3.5.0/
wget http://adslab.cse.cuhk.edu.hk/software/openec/apache-maven-3.5.0-bin.tar.gz
tar -zxvf apache-maven-3.5.0-bin.tar.gz

sudo apt-get install openjdk-8-jdk

#cd ~/
#wget http://adslab.cse.cuhk.edu.hk/software/openec/hadoop-3.0.0-src.tar.gz
#tar -zxvf hadoop-3.0.0-src.tar.gz


# sudo dpkg-reconfigure dash
# source ~/.bashrc
# Input "source ~/.bashrc" after running the script
