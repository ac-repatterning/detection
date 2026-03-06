"""Module gap.py"""

import datetime

import numpy as np
import pandas as pd


# noinspection DuplicatedCode
class Gap:
    """

    <b>Vis-à-vis raw measures series</b>
    --------------------------------<br>

    Context: Cases whereby N or more consecutive points have a NaN value.
    """

    def __init__(self, arguments: dict):
        """

        :param arguments:
        """

        self.__arguments = arguments
        self.__hours = 15
        self.__settings: dict = arguments.get('detecting').get('gap')

    def __get_reference(self, minimum: pd.Timestamp, maximum: pd.Timestamp):
        """

        :param minimum:
        :param maximum:
        :return:
        """

        # upper limit
        _until = datetime.datetime.now() - datetime.timedelta(hours=self.__hours)
        until = datetime.datetime.strptime(_until.strftime(format='%Y-%m-%d %H') + ':00:00', '%Y-%m-%d %H:%M:%S')

        # reference range
        dates = pd.date_range(start=minimum, end=max(until, maximum), freq=self.__arguments.get('frequency'),
                              inclusive='both', name='date', unit='ms').to_frame()
        dates.reset_index(drop=True, inplace=True)

        return dates

    # pylint: disable=R0801
    @staticmethod
    def __get_boundaries(_data: pd.Series) -> np.ndarray:
        """

        :param _data:
        :return:
        """

        # Set NaN points to 1?
        __values = _data.isna().astype(int)

        # Subsequently, the values that were not NaN values are set to NaN
        __set_irrelevant = __values.where(__values == 1, np.nan)
        intermediary = pd.concat([__set_irrelevant, pd.Series([np.nan])])

        # The difference between real values; always zero because all the real values are 1
        difference = intermediary.diff()

        # In aid of boundary determination
        constants=np.where(difference == 0, 1, np.nan)

        # Therefore
        conditionals = np.isnan(constants)
        exists = ~conditionals
        c_exists = np.cumsum(exists)
        __boundaries = np.diff(np.concatenate(([0], c_exists[conditionals])))

        boundaries = np.nan * np.zeros_like(constants)
        boundaries[conditionals] = __boundaries

        return boundaries[1:]

    def exc(self, data: pd.DataFrame) -> pd.DataFrame:
        """

        :param data:
        :return:
        """

        if data.empty:
            return data

        frame = data.copy()

        # Get the reference set of dates; Sequence(minimum, maximum, every N minutes).  Ascertain a strictly monotonically
        # increasing date sequence.
        dates: pd.DataFrame = self.__get_reference(minimum=frame['date'].min(), maximum=frame['date'].max())
        dates = dates.assign(timestamp=dates['date'].to_numpy().view('int64'))

        frame = frame.copy().merge(dates[['timestamp']], how='right', on='timestamp')
        frame.sort_values(by='timestamp', ascending=True, inplace=True)

        # Hence
        __frame = pd.DataFrame(data={'original': frame['original'].values})
        conditions = __frame['original'].isnull()
        __frame.loc[conditions, 'original'] = np.nan

        __frame['boundary'] = self.__get_boundaries(_data=__frame['original'])
        __frame['element'] = __frame['boundary'].bfill()
        __frame['gap'] = np.where(__frame['element'] >= (self.__settings.get('length') - 1),
                                  __frame['element'] + 1, 0)

        frame = frame.assign(gap=__frame['gap'].values, missing=__frame['original'].isna().astype(int))

        return frame
