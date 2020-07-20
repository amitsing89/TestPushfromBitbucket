# TAAS
This is a solution to anonymise the data sets to random output.

# Description
This project is developed to create random tokens for any type of input say phone numbers, credit card numbers etc with the ability 
to preserve last or first n digits as in the request.

We use crypto.randomBytes() to generate random numbers which is unlikely to be unique.

The services are written using REST architecture and exposed through json web tokens for first time login. We authorize the user with username
and password is RSA fingerprint entry in a keystore file.

The data vault is Oracle database.

# Pre-Requisites
Software ---- Version(localhost)
 node.js - 7.2.0 
 mongo - x.x.x
 oracle - any version(tested on XE)

Software ---- Version(UAT server)
nodejs - 0.12.0(older version is only compatible with gcc compiler version <4, issues with node oracledb driver compatiblilty)
oracle - 11c


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
2.	Cd /app/ur/tokenization_app/taas
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