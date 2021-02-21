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
class SortOrderBy:

    logging.config.fileConfig(str(get_project_root())+"/resources/configs/logging.conf")
    def run_pipeline(self):
        try:
            logging.info("https://sparkbyexamples.com/pyspark-tutorial/")
            logging.info('run_pipeline method started --> https://sparkbyexamples.com/pyspark/pyspark-orderby-and-sort-explained/')
            simpleData = [("James", "Sales", "NY", 90000, 34, 10000), \
                          ("Michael", "Sales", "NY", 86000, 56, 20000), \
                          ("Robert", "Sales", "CA", 81000, 30, 23000), \
                          ("Maria", "Finance", "CA", 90000, 24, 23000), \
                          ("Raman", "Finance", "CA", 99000, 40, 24000), \
                          ("Scott", "Finance", "NY", 83000, 36, 19000), \
                          ("Jen", "Finance", "NY", 79000, 53, 15000), \
                          ("Jeff", "Marketing", "CA", 80000, 25, 18000), \
                          ("Kumar", "Marketing", "NY", 91000, 50, 21000) \
                          ]
            # Sort by - sorting happens in specific partitons. Order is not guaranted
            #Group by  - order is gurantee
            columns = ["employee_name", "department", "state", "salary", "age", "bonus"]
            df = self.spark.createDataFrame(data=simpleData, schema=columns)
            df.printSchema()
            df.show(truncate=False)
            #Default is ascending order
            df.sort(df.department.asc(), df.state.asc()).show(truncate=False)
            df.sort("department","state").show(truncate=False)
            df.sort(F.col("department").asc(), F.col("state").desc()).show(truncate=False)

            df.orderBy(df.department.asc(), df.state.desc()).show(truncate=False)
            df.orderBy("department", "state").show(truncate=False)
            df.orderBy(F.col("department").asc(), F.col("state").desc()).show(truncate=False)

            #Nulls first or last function  ascending order
            df.sort(df.department.asc_nulls_first(), df.state.desc_nulls_first()).show(truncate=False)
            df.sort("department","state").show(truncate=False)
            df.sort(F.col("department").asc_nulls_last()(), F.col("state").desc_nulls_last()).show(truncate=False)

            df.orderBy(df.department.asc(), df.state.desc()).show(truncate=False)
            df.orderBy("department", "state").show(truncate=False)
            df.orderBy(F.col("department").asc(), F.col("state").desc()).show(truncate=False)

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
    pipeline = SortOrderBy()
    pipeline.verifyUsage(sys.argv[1:])
    pipeline.create_spark_session()
    pipeline.run_pipeline()
    logging.info('Pipeline executed')



