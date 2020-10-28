#!/usr/bin/env bash
#Service name in the form of input from command prompt/Terminal
service="$1"
#Process Checking of the service
ps_out=`ps -ef | grep $1 | grep -v 'grep' | grep -v $0`
echo $ps_out
#Alert Mechanism
if [ "${ps_out:-null}" = null ]; then
    echo "not running"
    #Mailer can be implemented here in case service is not running
    #mail -s Test-Email $2
else
    echo "running"
fi
