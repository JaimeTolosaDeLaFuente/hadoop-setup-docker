hdfs dfs -ls /
hdfs dfs -ls /user
hdfs dfs -ls /user/training
hdfs dfs -mkdir /devsh_loudacre
cd $DEVDATA
hdfs dfs -put activations /devsh_loudacre/
hdfs dfs -ls /devsh_loudacre/activations/
hdfs dfs -cat /devsh_loudacre/activations/2014-03.xml | head -n 20
hdfs dfs -get /devsh_loudacre/activations/ /tmp/devsh_activations
ls /tmp/devsh_activations
hdfs dfs -rm -r /devsh_loudacre/activations/
