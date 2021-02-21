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


class Joins:

    logging.config.fileConfig(str(get_project_root())+"/resources/configs/logging.conf")
    def run_pipeline(self):
        try:
            logging.info("https://github.com/khajaasmath786/pyspark-examples/blob/master/pyspark-join.py")
            logging.info('run_pipeline method started --> https://sparkbyexamples.com/pyspark/pyspark-explode-array-and-map-columns-to-rows/')
            emp = [(1, "Smith", -1, "2018", "10", "M", 3000), \
                   (2, "Rose", 1, "2010", "20", "M", 4000), \
                   (3, "Williams", 1, "2010", "10", "M", 1000), \
                   (4, "Jones", 2, "2005", "10", "F", 2000), \
                   (5, "Brown", 2, "2010", "40", "", -1), \
                   (6, "Brown", 2, "2010", "50", "", -1) \
                   ]
            empColumns = ["emp_id", "name", "superior_emp_id", "year_joined", \
                          "emp_dept_id", "gender", "salary"]

            empDF = self.spark.createDataFrame(data=emp, schema=empColumns)
            empDF.printSchema()
            empDF.show(truncate=False)
            from pyspark.sql.functions import col
            dept = [("Finance", 10), \
                    ("Marketing", 20), \
                    ("Sales", 30), \
                    ("IT", 40) \
                    ]
            deptColumns = ["dept_name", "dept_id"]
            deptDF = self.spark.createDataFrame(data=dept, schema=deptColumns)
            deptDF.printSchema()
            deptDF.show(truncate=False)
            df5=empDF.alias("emp1").join(empDF.alias("emp2"), \
                                     col("emp1.superior_emp_id") == col("emp2.emp_id"), "inner")
            df5.printSchema()

            empDF.alias("emp1").join(empDF.alias("emp2"), \
                                     col("emp1.superior_emp_id") == col("emp2.emp_id"), "inner") \
                .select(col("emp1.emp_id"), col("emp1.name"), \
                        col("emp2.emp_id").alias("superior_emp_id"), \
                        col("emp2.name").alias("superior_emp_name")) \
                .show(truncate=False)

            empDF.createOrReplaceTempView("EMP")
            deptDF.createOrReplaceTempView("DEPT")

            joinDF = self.spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id") \
                .show(truncate=False)

            joinDF2 = self.spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id") \
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
    pipeline = Joins()
    pipeline.verifyUsage(sys.argv[1:])
    pipeline.create_spark_session()
    pipeline.run_pipeline()
    logging.info('Pipeline executed')


