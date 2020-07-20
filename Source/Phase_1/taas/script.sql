----------sql scripts to start the oracle database-------------

Windows:
cmd> sqlplus / as sysdba
SQL> STARTUP  


/*** create table data_vault ***/
sqlplus
SQL>CREATE table data_vault (PLAIN_TEXT VARCHAR2(40) , TOKEN_VALUE VARCHAR2(40) );


UAT server OS details:
Enterprise Linux Enterprise Linux Server release 5.6 (Carthage)
Oracle Linux Server release 5.6
Red Hat Enterprise Linux Server release 5.6 (Tikanga) 
GCC version : 4.1

95 - M5Y}4v)z
'tOG6k9>

#Other commands useful for testing
sudo npm cache clean -f
sudo npm install -g n

sudo chown -R $(urpetl01) $(npm config get prefix)/{lib/node_modules,bin,share}
sudo chown -R urpetl01 $(npm config get prefix)/{lib/node_modules,bin,share}

sudo chown -R urpetl01 /usr/local/lib/node_modules

sudo chown -R  urpetl01 /home/abidih

sudo chown urpetl01 /var/run/screen


export http_proxy=proxy.intra.bt.com:8080 
export https_proxy=proxy.intra.bt.com:8080 
wget --no-check-certificate https://nodejs.org/download/release/v0.12.5/node-v0.12.5-linux-x64.tar.gz 
tar --strip-components 1 -xzf /app/ur/software/node-v0.12.5-linux-x64.tar.gz 

cd /usr/local 
tar --strip-components 1 -xzf /home/abidih/node-v6.9.1-linux-x64.tar.gz

http://www.thegeekstuff.com/2015/10/install-nodejs-npm-linux/

forever stop ./bin/www

forever start  --minUptime 1000 --spinSleepTime 1000 ./bin/www

using nohup due to 10% directory full issue
nohup node ./bin/www &