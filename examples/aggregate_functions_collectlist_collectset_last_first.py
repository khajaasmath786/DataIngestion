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


class AggregateFunctions:

    logging.config.fileConfig(str(get_project_root())+"/resources/configs/logging.conf")
    def run_pipeline(self):
        try:
            logging.info("https://sparkbyexamples.com/pyspark/pyspark-aggregate-functions/")
            # check collect_list and collect_set
            #collect_set() function returns all values from an input column with duplicate values eliminated.
            #collect_list() function returns all values from an input column with duplicates

            logging.info('run_pipeline method started --> https://sparkbyexamples.com/pyspark/pyspark-explode-array-and-map-columns-to-rows/')
            simpleData = [("James", "Sales", 3000),
                          ("Michael", "Sales", 4600),
                          ("Robert", "Sales", 4100),
                          ("Maria", "Finance", 3000),
                          ("James", "Sales", 3000),
                          ("Scott", "Finance", 3300),
                          ("Jen", "Finance", 3900),
                          ("Jeff", "Marketing", 3000),
                          ("Kumar", "Marketing", 2000),
                          ("Saif", "Sales", 4100)
                          ]
            schema = ["employee_name", "department", "salary"]

            df = self.spark.createDataFrame(data=simpleData, schema=schema).cache()
            df.show(truncate=False)

            from pyspark.sql.functions import approx_count_distinct, collect_list
            from pyspark.sql.functions import collect_set, sum, avg, max, countDistinct, count
            from pyspark.sql.functions import first, last, kurtosis, min, mean, skewness
            from pyspark.sql.functions import stddev, stddev_samp, stddev_pop, sumDistinct
            from pyspark.sql.functions import variance, var_samp, var_pop
            df.printSchema()
            df.show(truncate=False)

            print("approx_count_distinct: " + \
                  str(df.select(approx_count_distinct("salary")).collect()[0][0]))

            print("avg: " + str(df.select(avg("salary")).collect()[0][0]))

            df.select(collect_list("salary")).show(truncate=False)

            df.select(collect_set("salary")).show(truncate=False)

            df2 = df.select(countDistinct("department", "salary"))
            df2.show(truncate=False)
            print("Distinct Count of Department & Salary: " + str(df2.collect()[0][0]))

            print("count: " + str(df.select(count("salary")).collect()[0]))
            dffirst=df.select(first("salary"))
            dffirst.show(truncate=False)
            df.select(last("salary")).show(truncate=False)
            df.select(kurtosis("salary")).show(truncate=False)
            df.select(max("salary")).show(truncate=False)
            df.select(min("salary")).show(truncate=False)
            df.select(mean("salary")).show(truncate=False)
            df.select(skewness("salary")).show(truncate=False)
            df.select(stddev("salary"), stddev_samp("salary"), \
                      stddev_pop("salary")).show(truncate=False)
            df.select(sum("salary")).show(truncate=False)
            df.select(sumDistinct("salary")).show(truncate=False)
            df.select(variance("salary"), var_samp("salary"), var_pop("salary")) \
                .show(truncate=False)

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
    pipeline = AggregateFunctions()
    pipeline.verifyUsage(sys.argv[1:])
    pipeline.create_spark_session()
    pipeline.run_pipeline()
    logging.info('Pipeline executed')



