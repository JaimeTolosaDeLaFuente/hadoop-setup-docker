#!/bin/bash

hadoop fs -mkdir /user/training
hadoop fs -mkdir /devsh_loudacre

if [ $1 -le 1 ]
then
    echo "Cargando datos del tema 1"
    hadoop fs -put /home/devsh/data/activations /devsh_loudacre/
    hadoop fs -put /home/devsh/data/kb /devsh_loudacre/
elif [ $1 -le 2 ]
then
    echo "Cargando datos del tema 2"
    hadoop fs -put /home/devsh/data/devices.json /devsh_loudacre/
else
    echo "No coincide el valor númerico con ningun tema"
fi
