# upload data file if not previously uploaded: hdfs dfs -put $DEVDATA/kb /devsh_loudacre/
hdfs dfs -put $DEVDATA/kb /devsh_loudacre/
hdfs dfs -ls /devsh_loudacre/kb
spark-submit $DEVSH/exercises/yarn/wordcount.py /devsh_loudacre/kb/*
yarn application -list
yarn application -list -appStates ALL