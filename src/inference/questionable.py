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

        self.__arguments = arguments
        self.__factor = 0.01

        # Future
        key_name = f'{self.__arguments.get('prefix').get('metrics')}/metrics/aggregates/by_stage.json'
        __aggregates = src.s3.serials.Serials(
            connector=connector, bucket_name=s3_parameters.external).objects(key_name=key_name)
        frame = pd.json_normalize(data=__aggregates.get('testing'), record_path='data')
        self.__aggregates = frame.set_axis(labels=__aggregates.get('testing').get('columns'), axis=1)

    def __plausible_anomalies(self, frame: pd.DataFrame, ts_id: int) -> np.ndarray:
        """

        :param frame:
        :param ts_id:
        :return:
        """

        points: np.ndarray = frame['p_error'].values
        real: np.ndarray = frame['original'].notna().values

        # Quantiles & Boundaries
        quantiles = self.__aggregates.loc[self.__aggregates['ts_id'] == ts_id, :][:1].squeeze()
        median = quantiles.get('median_pe')
        l_limit = quantiles.get('l_whisker_pe_extreme')
        u_limit = quantiles.get('u_whisker_pe_extreme')
        l_boundary = l_limit - ((self.__factor/l_limit) * (median - l_limit))
        u_boundary = u_limit + ((self.__factor/u_limit) * (u_limit - median))

        # An anomaly vis-à-vis quantiles metrics?
        p_outliers = np.where((points < l_boundary) | (points > u_boundary), 1, 0)
        p_anomalies = np.where(p_outliers & real, 1, 0)

        return p_anomalies

    def exc(self, estimates: pd.DataFrame, specification: sc.Specification):
        """

        :param estimates:
        :param specification:
        :return:
        """

        frame = estimates.copy()
        p_anomalies = self.__plausible_anomalies(frame=frame.copy(), ts_id=specification.ts_id)
        frame = frame.assign(p_anomaly=p_anomalies)

        return frame
