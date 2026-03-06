"""Module persist.py"""
import json
import os

import pandas as pd

import config
import src.elements.specification as sc
import src.functions.objects


class Persist:
    """
    Persist
    """

    def __init__(self):
        """
        Constructor
        """

        self.__configurations = config.Config()

        # An instance for writing JSON objects
        self.__objects = src.functions.objects.Objects()

    @staticmethod
    def __get_node(blob: pd.DataFrame) -> dict:
        """

        :param blob:
        :return:
        """

        string: str = blob.to_json(orient='split')

        return json.loads(string)

    @staticmethod
    def __p_anomalies(estimates: pd.DataFrame) -> pd.DataFrame:
        """

        :param estimates:
        :return:
        """

        if 'p_anomaly' in list(estimates.columns):
            p_anomalies: pd.DataFrame = estimates.copy().loc[
                estimates['p_anomaly'] != 0, ['timestamp', 'original', 'measure', 'p_anomaly']]
            p_anomalies: pd.DataFrame = p_anomalies.copy().loc[p_anomalies['p_anomaly'].notnull(), :]
        else:
            p_anomalies = pd.DataFrame()

        return p_anomalies

    def __persist(self, nodes: dict, name: str) -> str:
        """

        :param nodes: Dictionary of data.
        :param name: A name for the file.
        :return:
        """

        return self.__objects.write(
            nodes=nodes, path=os.path.join(self.__configurations.points_, f'{name}.json'))

    def exc(self, specification: sc.Specification, estimates: pd.DataFrame) -> pd.DataFrame:
        """

        :param specification: <br>
        :param estimates: <br>
        :return:
        """

        __estimates = estimates.copy()

        p_anomalies = self.__p_anomalies(estimates=__estimates.copy())
        gaps = __estimates.copy().loc[__estimates['gap'] != 0, ['timestamp', 'original', 'measure', 'gap']]
        missing = __estimates.copy().loc[__estimates['missing'] != 0, ['timestamp', 'original', 'measure', 'missing']]

        asymptotes = __estimates.copy().loc[__estimates['asymptote'] != 0, ['timestamp', 'original', 'measure', 'asymptote']]
        asymptotes = asymptotes.copy().loc[asymptotes['asymptote'].notnull(), :]

        nodes = {
            'estimates': self.__get_node(blob=__estimates.drop(columns=['date', 'ts_id'])),
            'p_anomalies': self.__get_node(blob=p_anomalies),
            'gaps': self.__get_node(blob=gaps),
            'missing': self.__get_node(blob=missing),
            'asymptotes': self.__get_node(blob=asymptotes)
        }
        nodes.update(specification._asdict())

        self.__persist(nodes=nodes, name=str(specification.ts_id))

        return estimates
