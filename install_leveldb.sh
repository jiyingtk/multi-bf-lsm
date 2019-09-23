make clean && make -j
sudo rm /usr/lib/libleveldb.so.1*
sudo rm /usr/lib/libleveldb.so
sudo cp -r include/leveldb /usr/include
sudo cp out-shared/libleveldb.so.1.20 /usr/lib
sudo cp out-static/libleveldb.a /usr/lib
cp out-static/libleveldb.a ./
cd /usr/lib
sudo ln -fs libleveldb.so.1.20 libleveldb.so.1
sudo ln -fs libleveldb.so.1 libleveldb.so
sudo ldconfig
