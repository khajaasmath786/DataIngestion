import sys
from pstats import SortKey

from pyspark.sql import SparkSession
from pipeline import transform, persist, ingest
import logging
import logging.config
from pipeline.utils import get_project_root
import sys
import getopt
import configparser
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,ArrayType
from pyspark.sql.functions import *
from pyspark.sql import functions as F
class ExplodeMapArraysToRows:

    logging.config.fileConfig(str(get_project_root())+"/resources/configs/logging.conf")
    def run_pipeline(self):
        try:
            logging.info("https://sparkbyexamples.com/pyspark-tutorial/")
            logging.info('run_pipeline method started --> https://sparkbyexamples.com/pyspark/pyspark-explode-array-and-map-columns-to-rows/')
            arrayData = [
                ('James', ['Java', 'Scala'], {'hair': 'black', 'eye': 'brown'}),
                ('Michael', ['Spark', 'Java', None], {'hair': 'brown', 'eye': None}),
                ('Robert', ['CSharp', ''], {'hair': 'red', 'eye': ''}),
                ('Washington', None, None),
                ('Jefferson', ['1', '2'], {})]

            df = self.spark.createDataFrame(data=arrayData, schema=['name', 'knownLanguages', 'properties'])
            df.printSchema()
            df.show()

            from pyspark.sql.functions import explode
            df2 = df.select(df.name, explode(df.knownLanguages))
            df2.printSchema()
            df2.show()
            df3 = df.withColumn("ExplodedColumn", explode(df.knownLanguages))
            df3.printSchema()
            df3.show()

            # Exploding map and Array
            from pyspark.sql.functions import explode

            logging.info("Asmath --> Only one generator allowed per select clause but found 2: explode(knownLanguages), explode(properties);")
            #Error: Only one generator allowed per select clause but found 2: explode(knownLanguages), explode(properties);
            #df5 = df.select(df.name, explode(df.knownLanguages), explode(df.properties))
            df5 = df.withColumn("ExplodedArrayColumn", explode(df.knownLanguages))
            df5.printSchema() # it wont throw error if you dont pass () but it wont print schema
            df6=df5.withColumn("ExplodedMapColumn", explode(df5.properties)) # pass df5 here not df
            df6.printSchema()
            df6.show()


            # Explode Array

            logging.info('run_pipeline method ended')
        except Exception as exp:
            logging.error("An error occured while running the pipeline > " +str(exp) )
            # send email notification
            # log error to database
            sys.exit(1)

        return

    def create_spark_session(self):
        app_name = self.file_config.get('APP_CONFIGS', 'APP_NAME')
        self.spark = SparkSession.builder\
            .appName(str(app_name))\
            .config("spark.driver.extraClassPath","pipeline/postgresql-42.2.18.jar")\
            .enableHiveSupport().getOrCreate()

    def create_hive_table(self):
        self.spark.sql("create database if not exists fxxcoursedb")
        self.spark.sql("create table if not exists fxxcoursedb.fx_course_table (course_id string,course_name string,author_name string,no_of_reviews string)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (1,'Java','FutureX',45)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (2,'Java','FutureXSkill',56)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (3,'Big Data','Future',100)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (4,'Linux','Future',100)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (5,'Microservices','Future',100)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (6,'CMS','',100)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (7,'Python','FutureX','')")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (8,'CMS','Future',56)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (9,'Dot Net','FutureXSkill',34)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (10,'Ansible','FutureX',123)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (11,'Jenkins','Future',32)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (12,'Chef','FutureX',121)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (13,'Go Lang','',105)")
        #Treat empty strings as null
        self.spark.sql("alter table fxxcoursedb.fx_course_table set tblproperties('serialization.null.format'='')")

    def verifyUsage(self,arguments):
        self.config_file = ''
        self.file_config=None
        try:
            opts, args = getopt.getopt(arguments, "c:")
        except getopt.GetoptError:
            logging.error('test.py -c <inputfile> ')
            sys.exit(2)
        for opt, arg in opts:
            if opt not in ("-c"):
                logging.error('test.py -c <configfile>  ')
                sys.exit()
            elif opt == '-h':
                logging.info('test.py -c <configfile>  ')
            elif opt in ("-c"):
                self.config_file = arg
                self.file_config = configparser.ConfigParser()
                self.file_config.read(str(get_project_root())+"/resources/pipeline.ini")
        logging.info('Input file is '+str(self.config_file))
        logging.info('file config is '+str(self.file_config))


if __name__ == '__main__':
    logging.info('Application started')
    pipeline = ExplodeMapArraysToRows()
    pipeline.verifyUsage(sys.argv[1:])
    pipeline.create_spark_session()
    pipeline.run_pipeline()
    logging.info('Pipeline executed')



