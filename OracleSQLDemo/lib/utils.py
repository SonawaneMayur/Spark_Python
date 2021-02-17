"""
    File name:utils.py
    Author: Mayur Sonawane
    Date created: 9/10/2021
    Python Version: 3.8.3
    Description: Spark application utility functions
"""

import os

from pyspark import SparkConf

from . spark_configs import SPARK_APP_CONFIGS, DB_CONFIGS

config = None


def load_df(spark, data_file_expr):
    """
    Load data into Spark Dataframes
    :param spark: spark session
    :param data_file_expr: input file/ fo
    :return:
    """
    if "json" in data_file_expr:
        return spark.read \
            .json(data_file_expr, multiLine=True)
    else:
        return None


def count_by_country(survey_df):
    return survey_df.filter("Age < 40") \
        .select("Age", "Gender", "Country", "state") \
        .groupBy("Country") \
        .count()


def get_spark_app_config():
    spark_conf = SparkConf()
    for (key, val) in SPARK_APP_CONFIGS.items():
        spark_conf.set(key, val)
    return spark_conf

    # config = configparser.ConfigParser()
    # config.read("spark.conf")
    #
    # for (key, val) in config.items("SPARK_APP_CONFIGS"):
    #     spark_conf.set(key, val)
    # return spark_conf


def get_config(config_key):
    """get config by name"""
    return DB_CONFIGS[config_key]


def get_db_config(db):
    """
    Get database configs
    :param db: database name
    :return: database configs url, user, password, driver
    """
    url = DB_CONFIGS[db]["url"].format(os.environ['DB_URL_USER'],
                                       os.environ['DB_URL_PASSWORD'])
    user = os.environ['DB_USER']
    password = os.environ['DB_PASSWORD']
    driver = DB_CONFIGS[db]["driver"]
    return url, user, password, driver


def read_table(spark, db, table_name):
    """
    Read table from DB
    :param spark: spark session object
    :param db: database name
    :param table_name: table name
    :return: Spark Dataframe of db.table name
    """
    url, user, password, driver = get_db_config(db)
    return spark.read.format('jdbc'). \
        options(
        url=url,
        dbtable=table_name,
        user=user,
        password=password,
        driver=driver). \
        load()


def write_table(spark_df, db, table_name):
    """
    Write Dataframe to DB table
    :param spark_df: spark dataframe
    :param db: database name
    :param table_name: table name
    :return: Spark dataframe of db.table name
    """
    url, user, password, driver = get_db_config(db)
    return spark_df.write.format('jdbc'). \
        options(
        url=url,
        dbtable=table_name,
        user=user,
        password=password,
        driver=driver). \
        mode('overwrite').save()


def test():
    return "TEST"
