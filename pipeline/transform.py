import pyspark
from pyspark.sql import SparkSession
import logging
import logging.config
from pipeline.utils import get_project_root
class Transform:
    logging.config.fileConfig(str(get_project_root())+"/resources/configs/logging.conf")

    def __init__(self,spark):
        self.spark=spark

    def transform_data(self,df):
        logger = logging.getLogger("Transform")
        logger.info("Transforming")
        logger.warning("Warning in Transformer")

        # drop all the rows having null values
        #df1 = df.na.drop()
        df1 = df.na.fill("Unknown", ["author_name"])
        df2 = df1.na.fill("0", ["no_of_reviews"])
        return df2