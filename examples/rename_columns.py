import sys
from pyspark.sql import SparkSession
from pipeline import transform, persist, ingest
import logging
import logging.config
from pipeline.utils import get_project_root
import sys
import getopt
import configparser
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import *
from pyspark.sql import functions as F
class RenameColumns:

    logging.config.fileConfig(str(get_project_root())+"/resources/configs/logging.conf")
    def run_pipeline(self):
        try:
            logging.info("https://sparkbyexamples.com/pyspark-tutorial/")
            logging.info('run_pipeline method started --> https://sparkbyexamples.com/pyspark/pyspark-rename-dataframe-column/')
            nested_data_df = [(('James', '', 'Smith'), '1991-04-01', 'M', 3000),
                      (('Michael', 'Rose', ''), '2000-05-19', 'M', 4000),
                      (('Robert', '', 'Williams'), '1978-09-05', 'M', 4000),
                      (('Maria', 'Anne', 'Jones'), '1967-12-01', 'F', 4000),
                      (('Jen', 'Mary', 'Brown'), '1980-02-17', 'F', -1)
                      ]
            schema = StructType([
                StructField('name', StructType([
                    StructField('firstname', StringType(), True),
                    StructField('middlename', StringType(), True),
                    StructField('lastname', StringType(), True)
                ])),
                StructField('dob', StringType(), True),
                StructField('gender', StringType(), True),
                StructField('salary', IntegerType(), True)
            ])
            df = self.spark.createDataFrame(data=nested_data_df, schema=schema)
            df.printSchema()
            df.show()

            df2 = df.withColumnRenamed("dob", "DateOfBirth") \
                .withColumnRenamed("salary", "salary_amount")
            df2.printSchema()

            ## To Rename nested column, you need to pass the struct object once again and cast the original object
            schema2 = StructType([
                StructField("fname", StringType()),
                StructField("middlename", StringType()),
                StructField("lname", StringType())])
            df.select(F.col("name").cast(schema2),
                      F.col("dob"),
                      F.col("gender"),
                      F.col("salary")) \
                .printSchema()

            #Example 4 '''
            df.select(F.col("name.firstname").alias("fname"),
                      F.col("name.middlename").alias("mname"),
                      F.col("name.lastname").alias("lname"),
                      F.col("dob"), F.col("gender"), F.col("salary")) \
                .printSchema()
            df.show()
            # Using toDF
            newColumns = ["newCol1", "newCol2", "newCol3", "newCol4"]
            df.toDF("newCol1", "newCol2", "newCol3", "newCol4").printSchema()


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
    pipeline = RenameColumns()
    pipeline.verifyUsage(sys.argv[1:])
    pipeline.create_spark_session()
    pipeline.run_pipeline()
    logging.info('Pipeline executed')



