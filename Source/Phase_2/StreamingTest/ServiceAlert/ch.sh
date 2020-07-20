#!/bin/bash
# declare an array called array and define 3 vales
#array=( namenode datanode secondarynamenode nodemanager resourcemanager master worker pyspar )
#for i in "${array[@]}"
#do
#	echo $i
#done



#!/bin/bash
#Service name in the form of input from command prompt/Terminal
service="$1"
#Process Checking of the service
ps_out=`ps -ef | grep $1 | grep -v 'grep' | grep -v $0`
#declare -a arr=(namenode datanode secondarynamenode nodemanager resourcemanager master worker pyspar)
#for i in "${arr[@]}"
#do
#    ps_out=`ps -ef | grep $i | grep -v 'grep' | grep -v $0`
    if [ "${ps_out:-null}" = null ]; then
        echo $1 "---- >>> not running"
        service pyspark start
        #Mailer can be implemented here in case service is not running
        #mail -s Test-Email $2
    else
        echo $1"---- >>> running"
    fi
#done
#echo $ps_out
#Alert Mechanism
#if [ "${ps_out:-null}" = null ]; then
#    echo "not running"
    #Mailer can be implemented here in case service is not running
    #mail -s Test-Email $2
#else
#    echo $1 "---- >>> running"
#fi
