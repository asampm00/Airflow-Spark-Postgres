import pyspark.sql.dataframe
from pyspark.sql.functions import regexp_extract, countDistinct
import pandas as pd
import json
import os.path
import seaborn as sns
import jinja2


class InsightJob:
    """
    A Class to to run all the transformation jobs
    """

    def __init__(self, spark_session, helper_utils, config) -> None:
        """
        The default constructor
        :param spark_session: Spark Session instance
        :param helper_utils: Utils Class instance
        :param config: Config json
        """
        self.config = config
        self.report_path = config['report_path']
        self.spark_session = spark_session
        self.helper_utils = helper_utils
        self.df = None
        self.df_pantheon_EN = None
        self.df_role_EN = None

    def run(self) -> None:
        """
        Main transform class to run all jobs and save the data into absolute sink location
        """
        def to_retrieve_data() -> None:
            self.df = self.spark_session.read \
                .format("jdbc") \
                .option("url", self.config['postgres_db'])\
                .option("dbtable", self.config['postgres_table'])\
                .option("user", self.config['postgres_user'])\
                .option("password", self.config["postgres_pwd"])\
                .load()
            print(self.df.show(5))

        def get_role_EN() -> None:
            self.df_role_EN = self.df.select("role_EN").toPandas()

        def get_pantheon_EN() -> None:
            self.df_pantheon_EN = self.df.select("pantheon_EN").toPandas()            

        to_retrieve_data()

        get_role_EN()
        get_pantheon_EN()

        self.helper_utils.get_report(self.df_pantheon_EN, 'pantheon_EN.csv')
        self.helper_utils.get_report(self.df_role_EN, 'role_EN.csv')


class HelperUtils:
    """
    A helper utils class to support the necessary action trigger by pyspark and generate the report
    """

    @staticmethod
    def config_loader(file_path) -> json:
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

    @staticmethod
    def get_report(df_country : pd.DataFrame, report_path : str) -> None:
        df_country.to_csv('/usr/local/airflow/output/'+report_path, index=False)

