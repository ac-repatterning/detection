"""Module aggregates.py"""
import boto3
import pandas as pd

import src.elements.s3_parameters as s3p
import src.s3.serials


class Aggregates:
    """
    Reads-in training and testing stage error metrics.
    """

    def __init__(self, connector: boto3.session.Session, s3_parameters: s3p.S3Parameters, arguments: dict):
        """

        :param connector: An instance of boto3.session.Session<br>
        :param s3_parameters: The overarching S3 parameters settings of this
                              project, e.g., region code name, buckets, etc.<br>
        :param arguments: A set of arguments vis-à-vis computation & storage objectives.<br>
        """

        self.__connector = connector
        self.__s3_parameters = s3_parameters
        self.__arguments = arguments

    def __get_aggregates(self) -> pd.DataFrame:
        """

        :return:
        """

        # key name, data part
        key_name = f'{self.__arguments.get('prefix').get('metrics')}/metrics/aggregates/by_stage.json'
        via = self.__arguments.get('detecting').get('questionable').get('via')

        # reading the data in
        __aggregates = src.s3.serials.Serials(
            connector=self.__connector, bucket_name=self.__s3_parameters.external).objects(key_name=key_name)

        # focusing on the relevant part
        elements = pd.json_normalize(data=__aggregates.get(via), record_path='data')
        aggregates = elements.set_axis(labels=__aggregates.get(via).get('columns'), axis=1)

        return aggregates

    def __call__(self):
        """

        :return:
        """

        return self.__get_aggregates()
