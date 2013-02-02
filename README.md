Deduplication
=============

==========================
Installation Instructions
==========================
(i)   Install Sun java 7 
      Edit ~/.bashrc
      export PATH /home/vijay/java/jdk1.7.0_11/bin:$PATH
      bash

(ii)  Install Berkeley DB java
            
(iii) Memcached Installation:

      http://sacharya.com/using-memcached-with-java/

      1.Install Libevent
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

	$ memcached -d -m 512 127.0.0.1 -p 1121
	To stop use :   $ killall memcached

   CLASSPATH:
   export CLASSPATH=/home/vijay/Memcached/spymemcached-2.8.4.jar:/home/vijay/BDB/je-5.0.58/lib/je-5.0.58.jar:/home/vijay/java/jdk1.7.0_11/lib/tools.jar
