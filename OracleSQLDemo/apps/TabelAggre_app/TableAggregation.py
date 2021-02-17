"""
    File name:TableAggregation.py
    Author: Mayur Sonawane
    Date created: 9/10/2021
    Python Version: 3.8.3
    Description: This application is to load Oracle's Employee and Department tables, fetch the Department Summary,
    fetch the Employee records and save it locally in partitioned folder in JSON format
"""


from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from OracleSQLDemo.lib.logger import Log4j
from OracleSQLDemo.lib.utils import get_spark_app_config, read_table

if __name__ == "__main__":
    start_time = datetime.now()

    # the Spark session should be instantiated as follows

    conf = get_spark_app_config()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # Setting up logger
    logger = Log4j(spark)

    logger.info("Connecting Oracle DB....")
    try:
        emp_df = read_table(spark, "oracle", "EMP")
        dept_df = read_table(spark, "oracle", "DEPT")
        logger.info("Connection Successful!")

    except ConnectionError as c_error:
        logger.error("DB connection failed! ; Error - {}".format(c_error))

    except BaseException as b_error:
        logger.error("DB connection failed!; Error - {}".format(b_error))

    else:
        logger.info("Show all employees from EMP dataframe")
        emp_df.show()
        logger.info("Show all departments from DEPT dataframe")
        dept_df.show()

        logger.info("Print schema")
        emp_df.printSchema()
        dept_df.printSchema()

        dept_salary_df = emp_df \
            .groupBy("DEPTNO") \
            .agg(f.count("*").alias("NO_OF_EMP"),
                 f.sum("SAL").alias("DEPT_SAL"))
        dept_salary_df.show()

        join_expr = dept_df.DEPTNO == dept_salary_df.DEPTNO
        dept_summary_df = dept_df.join(dept_salary_df, join_expr, "right") \
            .select(dept_df.DEPTNO, dept_df.DNAME, dept_df.LOC, dept_salary_df.NO_OF_EMP, dept_salary_df.DEPT_SAL)
        dept_summary_df.show()

        logger.info("calculated Department summary")

        logger.debug("dept_summary_df columns - {}".format(dept_summary_df.columns))

        emp_df.write \
            .format("json") \
            .mode("overwrite") \
            .option("path", "dataSink/json/EMP/") \
            .partitionBy("DEPTNO") \
            .option("maxRecordsPerFile", 10000) \
            .save()

        logger.info("Saved EMP records at location {}".format("dataSink/json/EMP/"))

        dept_summary_df.coalesce(1).write \
            .format("json") \
            .mode("overwrite") \
            .option("path", "dataSink/json/DEPT/") \
            .option("maxRecordsPerFile", 10000) \
            .save()

        logger.info("Saved Department summary at location {}".format("dataSink/json/DEPT/"))

    finally:
        spark.stop()
        logger.info("Session stopped!")
        end_time = datetime.now()
        logger.info("Total time taken to execute jobs is {} seconds".format((end_time - start_time).seconds))


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
