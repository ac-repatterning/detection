"""Module questionable.py"""

import numpy as np
import pandas as pd

import src.elements.specification as sc


class Questionable:
    """
    Questionable
    """

    def __init__(self, aggregates: pd.DataFrame, arguments: dict):
        """
        :param aggregates: metrics<br>
        :param arguments:
        """

        self.__aggregates = aggregates
        self.__fraction = arguments.get('detecting').get('questionable').get('fraction')

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
        l_limit = self.__fraction * metrics.get('minimum_pe')
        u_limit = self.__fraction * metrics.get('maximum_pe')

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
