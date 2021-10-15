#!/bin/bash

DEVSH=/home/training/training_materials/devsh

# Numbers here 0-based, and 1-based in advance_exercises (on purpose)
setExerciseNames() {
  EXERCISES[0]="Starting the Exercise Environment"
  EXERCISES[1]="Working with HDFS"
  EXERCISES[2]="Running and Monitoring a YARN Job"
  EXERCISES[3]="Exploring DataFrames Using the Apache Spark Shell"
  EXERCISES[4]="Working with DataFrames and Schemas"
  EXERCISES[5]="Analyzing Data with DataFrame Queries"
  EXERCISES[6]="Working with RDDs"
  EXERCISES[7]="Transforming Data Using RDDs"
  EXERCISES[8]="Joining Data Using Pair RDDs"
  EXERCISES[9]="Querying Tables and Views with SQL"
  EXERCISES[10]="Using Datasets in Scala"
  EXERCISES[11]="Writing, Configuring, and Running a Spark Application"
  EXERCISES[12]="Exploring Query Execution"
  EXERCISES[13]="Persisting DataFrames"
  EXERCISES[14]="Implementing an Iterative Algorithm"
  EXERCISES[15]="Processing Streaming Data"
  EXERCISES[16]="Working with Apache Kafka Streaming Messages"
  EXERCISES[17]="Aggregating and Joining Streaming DataFrames"
}

getStartState() {
  validResponse=0
  while [ $validResponse -eq 0 ] 
  do 
    echo ""
    echo "Please enter the number of the exercise that you want to do."
    echo "This script will reset your system to the start state for that exercise."
    echo ""
    echo " 1" ${EXERCISES[0]}
    echo " 2" ${EXERCISES[1]} 
    echo " 3" ${EXERCISES[2]} 
    echo " 4" ${EXERCISES[3]}
    echo " 5" ${EXERCISES[4]}
    echo " 6" ${EXERCISES[5]}
    echo " 7" ${EXERCISES[6]}
    echo " 8" ${EXERCISES[7]}
    echo " 9" ${EXERCISES[8]}
    echo "10" ${EXERCISES[9]}
    echo "11" ${EXERCISES[10]}
    echo "12" ${EXERCISES[11]}
    echo "13" ${EXERCISES[12]}
    echo "14" ${EXERCISES[13]}
    echo "15" ${EXERCISES[14]}
    echo "16" ${EXERCISES[15]}
    echo "17" ${EXERCISES[16]}
    echo "18" ${EXERCISES[17]}
    echo ""
    read EXERCISE
    if [[ $EXERCISE -ge 1 && $EXERCISE -le 21 ]]; then
      PENULTIMATE=$((EXERCISE-1))
      validResponse=1
    else 
      echo ""
      echo "Invalid response. Please re-enter a valid exercise number." 
      echo ""
    fi
  done  
} 

doCatchup(){
  if [[ $EXERCISE -gt 1 ]]; then
    ADVANCE_TO=exercise$PENULTIMATE
  else
    ADVANCE_TO=cleanup
  fi
  $DEVSH/scripts/advance_exercises.sh $ADVANCE_TO
  echo ""
  echo "You can now perform the" ${EXERCISES[$PENULTIMATE]} "exercise."
  echo ""
}

setExerciseNames
getStartState
doCatchup
