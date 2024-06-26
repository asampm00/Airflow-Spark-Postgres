"""
Author: kamrul Hasan
Date: 17.04.2022
Email: hasan.alive@gmail.com
"""

"""
This module will transform the data using pyspark and load back the data to postgres to persist the data 
"""

from pyspark.sql.functions import udf, initcap, col, to_date
from pyspark.sql.types import StringType, StructField, StructType
import json


class ExtractJob:
    """
    A Class to to run all the transformation jobs
    """

    def __init__(self, spark_session, helper_utils, config) -> None:
        """
        The default constructor
        :param spark_session: Spark Session instance
        :param helper_utils: Utils Class instance
        :param config: Config data
        """
        self.config = config
        self.source_path = config['input_path']
        self.spark_session = spark_session
        self.helper_utils = helper_utils
        self.df = None
        self.df_valid = None

    def run(self) -> None:
        """
        Main transform class to run all jobs and save the data into jdbc sink (postgres)
        """
        def read_data() -> None:
            # read the input file
            self.df = self.spark_session.read.json(self.source_path)

        def to_persist_data() -> None:
            """
            Data persist to use for later into postgres
            """
            (self.df.write
             .format("jdbc")
             .option("url", self.config['postgres_db'])
             .option("dbtable", self.config['postgres_table'])
             .option("user", self.config['postgres_user'])
             .option("password", self.config["postgres_pwd"])
             .mode("overwrite")
             .save())

        read_data()
        to_persist_data()

class HelperUtils:
    """
    A helper class to provide some dependecy function to help the trasnform job
    """

    @staticmethod
    def validIPAddress(IP: str) -> str:
        """
        A very simple ip validation checker function
        """
        def isIPv4(s):
            try:
                return str(int(s)) == s and 0 <= int(s) <= 255
            except:
                return False

        def isIPv6(s):
            if len(s) > 4: return False
            try:
                return int(s, 16) >= 0 and s[0] != '-'
            except:
                return False

        if IP.count(".") == 3 and all(isIPv4(i) for i in IP.split(".")):
            return "valid"
        if IP.count(":") == 7 and all(isIPv6(i) for i in IP.split(":")):
            return "valid"
        return "invalid"

    @staticmethod
    def config_loader(file_path: str) -> json:
        """
        A function to load config file
        """
        try:
            with open(file_path, 'r') as f:
                config = json.load(f)
        except IOError:
            print("Error: File does not appear to exist.")
            return 0
        return config
