# TAAS
This is a data anonymisation solution to tokenize the data sets to values of same usefulness preserving the data type and format.

# Description
This project is developed to tokenize the sensitive information using techniques of masking, hashing, pseudo-randomization etc.
This solution runs on Node.js , nonblocking and asynchronous JS engine. The apis' are RESTful api's can be used on adhoc basis or plugged in with a GUI or can be consumed from 
any http utiliy client like postman or any user written code.

The persistence layer is Oracle database.This can also output data to nosql databases like mongo or reddis.

# Pre-Requisites
Software ---- Version(localhost)
 node.js - 7.2.0 
 mongo - x.x.x
 oracle - any version(tested on XE)

Software ---- Version(UAT server)
nodejs - 0.12.0(older version is only compatible with gcc compiler version <4, issues with node oracledb driver compatiblilty)
oracle - 11c

# installation of modules
Point your cmd to project folder,
taas> npm install

This will install all the required libraries defined in package.json

# Starting the App
To start the services , point your cmd to porject folder,
taas>npm start

# The server is up and running on localhost:3000
You can now hit each service and see the output either through postman https://documenter.getpostman.com/view/870052/taas/2Qrrop
or curl.

# ORACLE SERVER DETAILS: UAT SERVER 
hostname : 10.8.148.95 ( used to putty once you have access to your boatID )
connectionString: cbl11276clu01-scan-oravip.dci.bt.com:61901/usrpp_any
usernam: bt_model
password:bt

# DEV SERVER
hostname:172.25.178.163
connectionString: 172.25.178.163:61901/urepdev.nat.bt.com
username:bt_model
password:BT

sudo -i -u urpetl01 (on UAT server)

1.	Login with your boat id
2.	Cd /app/ur/CASE_POC/tokenization_app/taas
3.	export http_proxy=proxy.intra.bt.com:8080  
4.	export https_proxy=proxy.intra.bt.com:8080 

5.	export OCI_LIB_DIR=$ORACLE_HOME/lib
6.	export OCI_INC_DIR=$ORACLE_HOME/rdbms/public

7.	npm install --python=/usr/bin/python2.7 

# To create table in database , please refer script.sql


# MongoDB: only if you wish to connect locally
To start the mongo database:
$MONGO_HOME\bin>mongod.exe --dbpath ~\data
# data folder should be created before you start your database.


# authorization and authentication
Use of json web tokens to validate the user with username and passowrd as a java keystore file.
Integrated with kerberos authentication.