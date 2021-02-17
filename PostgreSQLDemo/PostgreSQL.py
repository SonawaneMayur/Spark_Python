from pyspark.sql import SparkSession

from lib.logger import Log4j

if __name__ == "__main__":
    # the Spark session should be instantiated as follows
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.jars", "../jars/postgresql-42.2.18.jar") \
        .getOrCreate()

    # list of the tables in the server
    table_names = spark.read.format('jdbc'). \
        options(
        url='jdbc:postgresql://localhost:5432/{db_name}',  # database url (local, remote)
        dbtable='information_schema.tables',
        user='{}',
        password='{}',
        driver='org.postgresql.Driver'). \
        load(). \
        filter("table_schema = 'public'").select("table_name")
    # DataFrame[table_name: string]

    # table_names_list.collect()
    # [Row(table_name='employee'), Row(table_name='department')]

    table_names_list = [row.table_name for row in table_names.collect()]
    print(table_names_list)
    # ['employee', 'department']






















