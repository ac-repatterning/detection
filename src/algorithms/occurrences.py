"""Module occurrences.py"""
import pandas as pd

import src.elements.specification as sc


class Occurrences:
    """

    The summary of the number of time points that a series' gaps, missing
    points, asymptotes, etc, spans.
    """

    def __init__(self):
        """

        Constructor
        """

        self.__names = ['p_anomaly', 'gap', 'missing', 'asymptote']

    def exc(self, frame: pd.DataFrame, specification: sc.Specification) -> dict:
        """

        :param frame:
        :param specification: Refer to src/elements/specification.py
        :return:
        """

        if frame.empty:
            return {}

        data: pd.DataFrame = frame.copy()[self.__names]
        matrix: pd.DataFrame = data.gt(0)
        vector = dict(matrix.sum(axis=0))

        vector.update(specification._asdict())
        vector['p_starting'] = frame['timestamp'].min()

        return vector
