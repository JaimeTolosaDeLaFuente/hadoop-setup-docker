#!/bin/bash
sudo rm -r /usr/local/hive/metastore_db

schematool -dbType derby -initSchema

CREATE TABLE accounts (id int, account_id int, device_id int, activation_date double, account_device_id string) row format delimited fields terminated by ','

LOAD DATA LOCAL INPATH '/home/devsh/data/accountdevice/part-00000-f3b62dad-1054-4b2e-81fd-26e54c2ae76a.csv' OVERWRITE INTO TABLE accounts;