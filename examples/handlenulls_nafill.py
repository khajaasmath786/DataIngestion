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


class HandleNulls:

    logging.config.fileConfig(str(get_project_root())+"/resources/configs/logging.conf")
    def run_pipeline(self):
        try:
            logging.info("https://sparkbyexamples.com/pyspark-tutorial/")
            logging.info("Asmath --> Easy usage is to follow the json format while replacing values")
            data = [("James", "", "Smith", "36636", "M", 60000),
                    ("Michael", "Rose", "", "40288", "M", 70000),
                    ("Robert", "", "Williams", "42114", "", 400000),
                    ("Maria", "Anne", "Jones", "39192", "F", 500000),
                    ("Jen", "Mary", "Brown", "", "F", 0)]

            columns = ["first_name", "middle_name", "last_name", "dob", "gender", "salary"]
            df = self.spark.createDataFrame(data=data, schema=columns)
            df.printSchema()
            df.show(truncate=False)

            # Using when otherwise
            from pyspark.sql.functions import col, when
            df2 = df.withColumn("new_gender", when(col("gender") == "M", "Male")
                                .when(col("gender") == "F", "Female")
                                .otherwise("Unknown"))
            df2.show(truncate=False)

            df22 = df.select(col("*"), when(col("gender") == "M", "Male")
                             .when(col("gender") == "F", "Female")
                             .otherwise("Unknown").alias("new_gender")).show(truncate=False)

            # Using case when
            from pyspark.sql.functions import expr
            df3 = df.withColumn("new_gender", expr("case when gender = 'M' then 'Male' " +
                                                   "when gender = 'F' then 'Female' " +
                                                   "else 'Unknown' end"))
            df3.show(truncate=False)

            # Using case when
            df4 = df.select(col("*"), expr("case when gender = 'M' then 'Male' " +
                                           "when gender = 'F' then 'Female' " +
                                           "else 'Unknown' end").alias("new_gender"))
            df4.show(truncate=False)

            data2 = [(66, "a", "4"), (67, "a", "0"), (70, "b", "4"), (71, "d", "4")]
            df5 = self.spark.createDataFrame(data=data2, schema=["id", "code", "amt"])

            df5.withColumn("new_column", when(col("code") == "a" | col("code") == "d", "A")
                           .when(col("code") == "b" & col("amt") == "4", "B")
                           .otherwise("A1")).show()










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
    pipeline = HandleNulls()
    pipeline.verifyUsage(sys.argv[1:])
    pipeline.create_spark_session()
    pipeline.run_pipeline()
    logging.info('Pipeline executed')



