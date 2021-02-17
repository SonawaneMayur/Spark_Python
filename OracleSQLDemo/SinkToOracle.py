"""
    File name:SinkToOracle.py
    Author: Mayur Sonawane
    Date created: 9/10/2021
    Python Version: 3.8.3
    Description: This jobs is to read data from FDA data in JSON,
    transform the expected data and load it to Oracle DB
"""

import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import expr
from pyspark.sql.types import DateType

from OracleSQLDemo.lib.logger import Log4j
from OracleSQLDemo.lib.utils import get_spark_app_config, load_df, write_table

if __name__ == "__main__":
    start_time = datetime.now()

    # the Spark session should be instantiated as follows
    conf = get_spark_app_config()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # Setting up logger
    logger = Log4j(spark)

    # Verify the arguments
    if len(sys.argv) != 2:
        logger.error("Usage: SinkToOracle <filename>")
        sys.exit(-1)

    logger.info("reading JSON files from folder....")
    try:
        # Form dataframe
        fda_df = load_df(spark, sys.argv[1])

        logger.info("Show all records from dataframe")
        fda_df.show()

        logger.info("Print schema")
        fda_df.printSchema()

        explode_data_df = fda_df.selectExpr("explode(data) as dataItem")
        # explode_metadata_df = fda_df.selectExpr("explode(metadata) as metadataItem")

        logger.info("Exploded form of data item")
        explode_data_df.printSchema()
        explode_data_df.show()

        logger.info("Select data items and assign column names")
        data_items_df = explode_data_df \
            .withColumn("published_date", expr("dataItem.published_date")) \
            .withColumn("setid", expr("dataItem.setid")) \
            .withColumn("spl_version", expr("dataItem.spl_version")) \
            .withColumn("title", expr("dataItem.title")) \
            .drop("dataItem")

        data_items_df.printSchema()
        data_items_df.show()
        logger.info("converting string column to date type")

        # User define function to parse date string to DateType
        func = f.udf(lambda x: datetime.strptime(x.strip(), "%b %d, %Y"), DateType())

        # Apply UDF to column for date type conversion
        data_items_df = data_items_df.withColumn('published_date', func(f.col('published_date')))
        data_items_df.printSchema()
        data_items_df.show()

        # Writing dataframe to Oracle DB
        write_table(data_items_df, "oracle", "FDA_ITEMS")

        logger.info("Saved FDA items to Oracle DB in {} table".format("FDA_ITEMS"))

    except ConnectionError as c_error:
        logger.error("DB connection failed!")
        logger.info(c_error)
    except BaseException as b_error:
        logger.error("DB connection failed!")
        logger.error(b_error)

    finally:
        spark.stop()
        logger.info("Session stopped!")
        end_time = datetime.now()
        logger.info("Total time taken to execute jobs is {} seconds".format((end_time - start_time).seconds))
