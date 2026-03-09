"""Module inference/interface.py"""

import boto3
import pandas as pd

import src.elements.attribute as atr
import src.elements.master as mr
import src.elements.specification as sc
import src.inference.approximating
import src.inference.questionable
import src.inference.scaling


class Interface:
    """
    Interface
    """

    def __init__(self, aggregates: pd.DataFrame, arguments: dict):
        """

        :param aggregates: A frame of key error metrics per gauge station; an instance per gauge station.<br>
        :param arguments: A set of arguments vis-à-vis computation & storage objectives.<br>
        """

        # Setting up
        self.__scaling = src.inference.scaling.Scaling()
        self.__approximating = src.inference.approximating.Approximating()
        self.__questionable = src.inference.questionable.Questionable(aggregates=aggregates, arguments=arguments)

    def exc(self, attribute: atr.Attribute, data: pd.DataFrame, specification: sc.Specification) -> pd.DataFrame:
        """

        :param attribute:
        :param data:
        :param specification:
        :return:
        """

        if data.empty | (not attribute.scaling) | (not attribute.modelling) :
            return data

        transforms: pd.DataFrame = self.__scaling.transform(data=data, scaling=attribute.scaling)
        master: mr.Master = mr.Master(data=data, transforms=transforms)
        estimates: pd.DataFrame = self.__approximating.exc(
            specification=specification, attribute=attribute, master=master)
        estimates: pd.DataFrame = self.__questionable.exc(
            estimates=estimates.copy(), specification=specification)

        return estimates
