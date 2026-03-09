"""Module questionable.py"""

import boto3
import numpy as np
import pandas as pd

import src.elements.s3_parameters as s3p
import src.elements.specification as sc
import src.s3.serials


class Questionable:
    """
    Differences
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

        # Metrics
        self.__aggregates = self.__get_aggregates()

    def __get_aggregates(self) -> pd.DataFrame:
        """

        :return:
        """

        key_name = f'{self.__arguments.get('prefix').get('metrics')}/metrics/aggregates/by_stage.json'
        via = self.__arguments.get('questionable').get('via')

        __aggregates = src.s3.serials.Serials(
            connector=self.__connector, bucket_name=self.__s3_parameters.external).objects(key_name=key_name)
        __frame = pd.json_normalize(data=__aggregates.get(via), record_path='data')
        frame = __frame.set_axis(labels=__aggregates.get(via).get('columns'), axis=1)

        return frame

    def __p_anomalies(self, frame: pd.DataFrame, ts_id: int) -> np.ndarray:
        """

        :param frame:
        :param ts_id:
        :return:
        """

        points: np.ndarray = frame['p_error'].values
        real: np.ndarray = frame['original'].notna().values

        # Metrics & Boundaries
        metrics = self.__aggregates.loc[self.__aggregates['ts_id'] == ts_id, :][:1].squeeze()
        l_limit = metrics.get('minimum_pe')
        u_limit = metrics.get('maximum_pe')

        # An anomaly vis-à-vis metrics?
        p_outliers = np.where((points < l_limit) | (points > u_limit), 1, 0)
        p_anomalies = np.where(p_outliers & real, 1, 0)

        return p_anomalies

    def exc(self, estimates: pd.DataFrame, specification: sc.Specification):
        """

        :param estimates:
        :param specification:
        :return:
        """

        frame = estimates.copy()
        p_anomalies = self.__p_anomalies(frame=frame.copy(), ts_id=specification.ts_id)
        frame = frame.assign(p_anomaly=p_anomalies)

        return frame
