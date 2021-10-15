#!/bin/bash

# This script will advance the state of the VM as if the
# exercise specified on the command line (and all those 
# before it) had been completed. For example, invoking 
# this script as:
#
#   $ advance_exercises.sh exercise4
#
# Will prepare you to begin work on exercise 5, meaning 
# that the state of the VM will be the same as if 
# exercise 4 (as well as exercises 3, 2, and 1) had been
# manually completed.
#
# BEWARE: In all invocations, this script will first run
#         a 'cleanup' step which removes data in HDFS and
#         the local filesystem in order to simulate the
#         original state of the VM. 
#

DEVSH=/home/training/training_materials/devsh
DEVDATA=/home/training/training_materials/devsh/data

# ensure we run any scripts from a writable local directory, 
# which needs to also not conflict with anything
RUNDIR=/tmp/devsh/exercisescripts/$RANDOM$$
mkdir -p $RUNDIR
cd $RUNDIR

# Kafka settings when running in a single-host environment
ZOOKEEPER_SERVERS=localhost:2181
KAFKA_REPLICATION=1


cleanup() {
    echo "Cleaning up your system"

    sudo -u hdfs hdfs dfs -rm  -skipTrash -r -f /devsh_loudacre

    hdfs dfs -rm  -skipTrash -r -f /tmp/kafka-checkpoint

    kafka-topics --if-exists  --zookeeper $ZOOKEEPER_SERVERS  --delete --topic weblogs 2&> /dev/null
    kafka-topics --if-exists  --zookeeper $ZOOKEEPER_SERVERS  --delete --topic activations-out 2&> /dev/null
    kafka-topics --if-exists  --zookeeper $ZOOKEEPER_SERVERS  --delete --topic activations 2&> /dev/null
}

exercise1() {
    echo "* Advancing through Exercise: Staring the Exercise Environment"
    # Nothing required for subsequent exercises
}

exercise2() {
    echo "* Advancing through Exercise: Working with HDFS"
    
    # required for later exercises
    sudo -u hdfs hdfs dfs -mkdir /devsh_loudacre
    sudo -u hdfs hdfs dfs -chmod +wr /devsh_loudacre
}

exercise3() {
    # YARN
    echo "* Advancing through Exercise: Running and Monitoring a YARN Job"

    # not required by might as well...
    hdfs dfs -put $DEVDATA/kb /devsh_loudacre/

}

exercise4() {
    echo "* Advancing through Exercise: Exploring DataFrames Using the Apache Spark Shell"
 
     # data required for this exercise and also for later exercises
    hdfs dfs -put $DEVDATA/devices.json /devsh_loudacre/

}


exercise5() {
    echo "* Advancing through Exercise: Working With DataFrames and Schemas"
    
    # Copy in solution data
    hdfs dfs -put $DEVDATA/static_data/accounts_zip94913/ /devsh_loudacre/
    hdfs dfs -put $DEVDATA/static_data/devices_parquet/ /devsh_loudacre/
}

exercise6() {
    echo "* Advancing through Exercise: Analyzing Data with DataFrame Queries"
    
    # data required to complete this exercise (also required for later exercises)
    hdfs dfs -put $DEVDATA/accountdevice/ /devsh_loudacre/
    hdfs dfs -put $DEVDATA/base_stations.parquet /devsh_loudacre/

    # Copy in solution data
    hdfs dfs -put $DEVDATA/static_data/top_devices/ /devsh_loudacre/

}

exercise7() {
    echo "* Advancing through Exercise: Working with RDDs"

    # data required to complete this exercise
    hdfs dfs -put $DEVDATA/frostroad.txt /devsh_loudacre/    
    hdfs dfs -put $DEVDATA/makes1.txt /devsh_loudacre/  
    hdfs dfs -put $DEVDATA/makes2.txt /devsh_loudacre/  

    # Solution data
    # No files saved in this exercise
}

exercise8() {
    echo "* Advancing through Exercise: Transforming Data Using RDDs"

    # This function sometimes causes HDFS warnings which can be disregarded
    # (to minimize this, we only upload data necessary for subsequent exercises)

    # data required to complete this exercise
    hdfs dfs -put $DEVDATA/weblogs/ /devsh_loudacre/  
    hdfs dfs -put $DEVDATA/activations/ /devsh_loudacre/  

    # Solution data
    #hdfs dfs -put $DEVDATA/static_data/iplist/ /devsh_loudacre/ 
    hdfs dfs -put $DEVDATA/static_data/devicestatus_etl /devsh_loudacre/
    #hdfs dfs -put $DEVDATA/static_data/userips_csv/ /devsh_loudacre/  
    #hdfs dfs -put $DEVDATA/static_data/account-models/ /devsh_loudacre/ 
}

exercise9() {
    echo "* Advancing through Exercise: Joining Data Using Pair RDDs"
    # No exercise output
}

exercise10() {
    echo "* Advancing through Exercise: Querying Tables and Views with SQL"

    # not required for later exercises, but students may want it later
    hdfs dfs -put $DEVDATA/static_data/name_dev/ /devsh_loudacre/  
}


exercise11() {
    echo "* Advancing through Exercise: Using Datasets in Scala"

    hdfs dfs -put $DEVDATA/static_data/accountIPs/ /devsh_loudacre/  
}

exercise12() {
    echo "* Advancing through Exercise: Writing, Configuring, and Running a Spark Application"
    
    # copying for student information, not required by subsequent exercises
    
    # Not copying accounts_by_state because content varies by state student chooses
    # hdfs dfs -put $DEVDATA/static-data/accounts_by_state /devsh_loudacre/
}

exercise13() {
    echo "* Advancing through Exercise: Exploring Query Execution"
    # No exercise output
}

exercise14() {
    echo "* Advancing through Exercise: Persisting Data"

    # not required for later exercises, but students may want it later
    hdfs dfs -put $DEVDATA/static_data/account_names_devices /devsh_loudacre/
}

exercise15() {
    echo "* Advancing through Exercise: Implementing an Iterative Algorithm with Apache Spark"
    # No exercise output
}

exercise16() {
    echo "* Advancing through Exercise: Introduction to Structured Streaming"
    # No output required by subsequent exercises
}

exercise17() {
    echo "* Advancing through Exercise: Structured Streaming with Apache Kafka"

    kafka-topics --create  --if-not-exists  --zookeeper $ZOOKEEPER_SERVERS  --replication-factor 1 --partitions 2   --topic activations
    kafka-topics --create  --if-not-exists  --zookeeper $ZOOKEEPER_SERVERS  --replication-factor 1 --partitions 2   --topic activations-out
    # No output required by subsequent exercises

}

exercise18() {
    echo "* Advancing through Exercise: Aggregating and Joining Streaming DataFrames"
    # No output required by subsequent exercises
}



case "$1" in
        
    cleanup)
        cleanup
        ;;

    exercise1)
        cleanup
        exercise1
        ;;

    exercise2)
        cleanup
        exercise1
        exercise2
        ;;

    exercise3)
        cleanup
        exercise1
        exercise2
        exercise3
        ;;

   exercise4)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        ;;

        
    exercise5)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        ;;

   exercise6)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        ;;

    exercise7)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        ;;

    exercise8)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
       ;;

    exercise9)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
        exercise9
        ;;

    exercise10)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
        exercise9
        exercise10
        ;;
        
    exercise11)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
        exercise9
        exercise10
        exercise11
        ;;
        
    exercise12)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
        exercise9
        exercise10
        exercise11
        exercise12
        ;;

    exercise13)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
        exercise9
        exercise10
        exercise11
        exercise12
        exercise13
        ;;

    exercise14)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
        exercise9
        exercise10
        exercise11
        exercise12
        exercise13
        exercise14
        ;;
        
    exercise15)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
        exercise9
        exercise10
        exercise11
        exercise12
        exercise13
        exercise14
        exercise15
        ;;

        
    exercise16)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
        exercise9
        exercise10
        exercise11
        exercise12
        exercise13
        exercise14
        exercise15
        exercise16
        ;;
                

        
    exercise17)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
        exercise9
        exercise10
        exercise11
        exercise12
        exercise13
        exercise14
        exercise15
        exercise16
        exercise17
        ;;
          
    exercise18)
        cleanup
        exercise1
        exercise2
        exercise3
        exercise4
        exercise5
        exercise6
        exercise7
        exercise8
        exercise9
        exercise10
        exercise11
        exercise12
        exercise13
        exercise14
        exercise15
        exercise16
        exercise17
        exercise18
        ;;         
    *)
        echo $"Usage: $0 {exercise1-exercise21}"
        exit 1

esac

