"""Module inference/interface.py"""

import multiprocessing

import boto3
import dask
import pandas as pd

import src.algorithms.asymptote
import src.algorithms.attributes
import src.algorithms.data
import src.algorithms.gap
import src.algorithms.occurrences
import src.algorithms.persist
import src.algorithms.perspective
import src.assets.menu
import src.elements.attribute as atr
import src.elements.s3_parameters as s3p
import src.elements.specification as sc
import src.inference.interface
import src.inference.aggregates


class Interface:
    """
    Interface
    """

    def __init__(self, connector: boto3.session.Session, s3_parameters: s3p.S3Parameters, arguments: dict):
        """

        :param connector: An instance of boto3.session.Session<br>
        :param s3_parameters: The overarching S3 parameters settings of this
                              project, e.g., region code name, buckets, etc.<br>
        :param arguments: A set of arguments vis-à-vis computation & storage objectives.<br>
        """

        self.__n_cores = multiprocessing.cpu_count()
        self.__aggregates = src.inference.aggregates.Aggregates(
            connector=connector, s3_parameters=s3_parameters, arguments=arguments)()

        # Setting up
        self.__get_attributes = dask.delayed(src.algorithms.attributes.Attributes().exc)
        self.__get_data = dask.delayed(src.algorithms.data.Data(arguments=arguments).exc)
        self.__gap = dask.delayed(src.algorithms.gap.Gap(arguments=arguments).exc)
        self.__asymptote = dask.delayed(src.algorithms.asymptote.Asymptote(arguments=arguments).exc)
        self.__get_special_estimates = dask.delayed(
            src.inference.interface.Interface(aggregates=self.__aggregates, arguments=arguments).exc)
        self.__persist = dask.delayed(src.algorithms.persist.Persist().exc)

    def exc(self, specifications: list[sc.Specification], reference: pd.DataFrame):
        """

        :param specifications:
        :param reference:
        :return:
        """

        __occurrences = dask.delayed(src.algorithms.occurrences.Occurrences().exc)


        computations = []
        for specification in specifications:
            attribute: atr.Attribute = self.__get_attributes(specification=specification)
            data: pd.DataFrame = self.__get_data(specification=specification)
            __estimates: pd.DataFrame = self.__get_special_estimates(
                attribute=attribute, data=data, specification=specification)
            __appending_gap: pd.DataFrame = self.__gap(data=__estimates)
            __appending_asymptote: pd.DataFrame = self.__asymptote(data=__appending_gap)
            estimates: pd.DataFrame = self.__persist(specification=specification, estimates=__appending_asymptote)

            vector: dict = __occurrences(frame=estimates, specification=specification)
            computations.append(vector)

        vectors: list[dict] = dask.compute(computations, scheduler='processes', num_workers=int(0.5*self.__n_cores))[0]
        records = pd.DataFrame.from_records(vectors)

        # Menu
        src.assets.menu.Menu().exc(
            reference=reference.loc[reference['ts_id'].isin(records['ts_id'].unique()), :])

        # An overarching perspective
        src.algorithms.perspective.Perspective().exc(records=records.copy())
