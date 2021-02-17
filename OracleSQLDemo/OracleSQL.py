from pyspark.sql import SparkSession

from lib.logger import Log4j

if __name__ == "__main__":
    # the Spark session should be instantiated as follows
    spark = SparkSession \
        .builder \
        .appName("PySpark_OracleSQL") \
        .config("spark.jars", "../jars/ojdbc6-11.2.0.4.jar") \
        .getOrCreate()

    # Setting up logger
    logger = Log4j(spark)

    logger.info("Connecting Oracle DB....")
    try:
        empDF = spark.read.format('jdbc'). \
            options(
            url='jdbc:oracle:thin:{}/{}@localhost:1521/XE',  # database url (local, remote)
            dbtable='EMP',
            user='{}',
            password='{}',
            driver='oracle.jdbc.driver.OracleDriver'). \
            load()
        logger.info("Connection Successful!")

    except ConnectionError as c_error:
        logger.error("DB connection failed!")
        logger.info(c_error)
    except BaseException as b_error:
        logger.error("DB connection failed!")
        logger.error(b_error)

    else:
        logger.info("Show all employees from EMP dataframe")
        empDF.show()

    finally:
        spark.stop()
        logger.info("Session stopped!")


# Use case-
#   1. Join 3 or more tables with different join type
#   2. Performing aggregation on multiple dataframes/datsets
#   3. Fuzzy string solutions with regular expression
#   4. Handling Slowly Changing Dimensions (SCD) Type 2 in Apache Spark
#   5. Best ETL practices with Python


## Multiple aggregation in spark
# stage 1 - stage 2 - stage 3
# lookup - all type joins
# type 2 - slowly changing data operation

# how to do ETL in Python



##### 1. read the data from Oracle table, apply join, aggregation, write it to JSON file

###### 2.  read csv file and load it into table

###### 3. read JSOn and insert to Oracle table

# year to date
# HR DB, payrol tables, emp table join with salary
