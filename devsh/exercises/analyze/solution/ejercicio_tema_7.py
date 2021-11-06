import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()

from pyspark.sql.types import *

col_acc = [
    StructField("acct_num", LongType()),
    StructField("acct_create_dt", StringType()),
    StructField("acct_close_dt", StringType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("address", StringType()),
    StructField("state", StringType()),
    StructField("zipcode", StringType()),
    StructField("phone_number", LongType()),
    StructField("created", StringType()),
    StructField("modified", StringType())
]

schema = StructType(col_acc)

devDF = spark.read.json("/devsh_loudacre/devices.json")
acdDF = spark.read.option("header", True).csv("/devsh_loudacre/accounts_devices.csv")
acc = spark.read.parquet("/devsh_loudacre/accounts.parquet")

acdDF = acdDF.withColumn("device_id", acdDF.device_id.cast(LongType()))

acc_ = acc.where(acc.acct_close_dt != "\\N")
df = devDF.select(devDF.devnum).join(acdDF.select(acdDF.device_id, acdDF.account_id), devDF.devnum == acdDF.device_id)
join_df =  df.join(acc_,acc_.acct_num == df.account_id).groupBy("device_id").count()
join_df = join_df.sort("count",ascending=False).withColumnRenamed("count","active_num")
top_devices = join_df.join(devDF.select(devDF.devnum, devDF.make, devDF.model), join_df.device_id == devDF.devnum).\
                select("device_id", "make", "model", "active_num")