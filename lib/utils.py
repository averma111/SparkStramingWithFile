from pyspark import SparkConf
import configparser


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("config/spark.conf")
    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


def read_from_stream(spark):
    return spark.readStream \
        .format("json") \
        .option("path", "input") \
        .option("maxFilePerTrigger","1") \
        .option("cleanSource","archieve") \
        .load()


def wrirte_to_console_from_stream(flattened_df):
    return flattened_df.writeStream \
        .format("json") \
        .option("checkpointLocation", "chk-point-dir") \
        .option("path","target") \
        .queryName("Flattened Invoice Writer") \
        .trigger(processingTime = "1 minute") \
        .start() 
