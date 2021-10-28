#!/bin/bash

hadoop fs -mkdir /user/training
hadoop fs -mkdir /devsh_loudacre

if [ $1 -ge 1 ]
then
    echo "Cargando datos del tema 1"
    hadoop fs -put /home/devsh/data/activations /devsh_loudacre/
    hadoop fs -put /home/devsh/data/kb /devsh_loudacre/
fi

if [ $1 -ge 2 ]
then
    echo "Cargando datos del tema 2"
    hadoop fs -put /home/devsh/data/devices.json /devsh_loudacre/
fi

if [ $1 -ge 3 ]
then
    echo "Cargando datos del tema 3"
    hadoop fs -put /home/devsh/data/accountdevice/accounts_devices.csv /devsh_loudacre/
    hadoop fs -put /home/devsh/data/base_stations.parquet /devsh_loudacre/
    hadoop fs -put /home/devsh/data/static_data/accounts /devsh_loudacre/
    hadoop fs -put /home/devsh/data/static_data/accounts_zip94913 /devsh_loudacre/
fi

echo "datos cargados"