parquet-tools schema $DEVDATA/base_stations.parquet
parquet-tools head $DEVDATA/base_stations.parquet
hdfs dfs -put $DEVDATA/base_stations.parquet /devsh_loudacre/
hdfs dfs -put $DEVDATA/accountdevice/ /devsh_loudacre/
hdfs dfs -get /devsh_loudacre/top_devices /tmp/
parquet-tools head /tmp/top_devices
