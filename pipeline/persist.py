import pyspark
from pyspark.sql import SparkSession
import sys
import psycopg2
from pipeline.utils import get_project_root
import logging
import logging.config
import configparser

class Persist:
    logging.config.fileConfig(str(get_project_root())+"/resources/configs/logging.conf")

    def __init__(self,spark):
        self.spark=spark

    def persist_data(self,df):
        try:
            logger = logging.getLogger("Persist")
            logger.info('Persisting')
            config = configparser.ConfigParser()
            config.read('pipeline/resources/pipeline.ini')
            target_table = config.get('DB_CONFIGS','TARGET_PG_TABLE')
            logger.info('PG Target table is '+str(target_table))

            #df.coalesce(1).write.option("header", "true").csv("transformed_retailstore")

            df.write\
            .mode("append")\
            .format("jdbc")\
            .option("url", "jdbc:postgresql://localhost:5432/postgres")\
            .option("dbtable", target_table)\
            .option("user", "postgres")\
            .option("password", "admin")\
            .save()

        except Exception as exp:
            logger.error("An error occured while persisiting data >"+str(exp))
            # store in database table
            # send an email notification
            raise Exception("HDFS directory already exists")
    def insert_into_pg(self):
        connection = psycopg2.connect(user='postgres', password='admin', host='localhost', database='postgres')
        cursor = connection.cursor()
        insert_query="INSERT INTO futurexschema.futurex_course_catalog (course_id, course_name, author_name, course_section, creation_date) VALUES (%s, %s, %s, %s,%s)"
        insert_tuple = (3,'Machine Learning','FutureX','{}', '2020-10-20')
        cursor.execute(insert_query, insert_tuple)
        cursor.close()
        connection.commit()

