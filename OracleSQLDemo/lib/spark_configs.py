"""
    File name:spark_configs.py
    Author: Mayur Sonawane
    Date created: 9/10/2021
    Python Version: 3.8.3
    Description: Define Spark application and databases configs
"""

SPARK_APP_CONFIGS = {
    "spark.app.name": "OracleDemo",
    "spark.master ": "local[3]",
    "spark.sql.shuffle.partitions": 2,
    "spark.jars": "../jars/ojdbc6-11.2.0.4.jar"
}

DB_CONFIGS = {
    "oracle": {
        "url": "jdbc:oracle:thin:{}/{}@localhost:1521/XE",
        "driver": "oracle.jdbc.driver.OracleDriver"
    },
    "postgres": {
        "url": "jdbc:postgresql://localhost:5432/testing",
        "driver": "org.postgresql.Driver"
    }

}





# "spark.scheduler.mode", "FAIR"
