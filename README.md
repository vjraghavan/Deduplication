Deduplication
=============

============
Installation
============



=======================
Memcached Installation:
=======================
http://sacharya.com/using-memcached-with-java/

1. Install Libevent
  Download Libevent
$ cd libevent-1.4.11-stable
$ autoconf
$ ./configure --prefix=/usr/local
$ make
$ sudo make install

2. Install Memcached

$ cd memcached-1.4.0
$ autoconf
$ ./configure --prefix=/usr/local
$ make
$ sudo make install

3. Run memcached:

$memcached -d -m 512 127.0.0.1 -p 1121
$killall memcached






