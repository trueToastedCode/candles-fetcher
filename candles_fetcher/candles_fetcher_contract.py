from abc import ABC, abstractmethod
from typing import Callable
from datetime import timedelta

from candles_fetcher.time_frame import TimeFrame

class CandlesFetcherContract(ABC):
    """
    An abstract base class defining the contract for candle fetchers.

    This contract specifies the methods that any candle fetcher implementation
    must provide. It ensures a consistent interface for initializing, running,
    and stopping the candle fetching process, both synchronously and asynchronously.

    The contract also defines the structure and behavior of the on_candles callback,
    ensuring consistency in how candle data is emitted across all implementations.
    """

    @abstractmethod
    def __init__(
        self,
        symbol     : str,
        time_frame : TimeFrame,
        on_candles : Callable[[list[dict]], None],
        max_size   : int        = 100,
        valid_delay: timedelta  = timedelta(seconds=20),
        *args,
        **kwargs
    ):
        """
        Initialize the candle fetcher.

        Args:
            symbol (str): The trading symbol for which to fetch candles.
            time_frame (TimeFrame): The time frame of the candles to fetch.
            on_candles (Callable[[list[dict]], None]): Callback function to be called with new candle data.
                This function should accept a single argument, which is a list.
                containing a dict with the keys:
                  'ot' (Opentime, datetime), 'o' (Open, float), 'h' (High, float),
                  'l' (Low, float), 'c' (Close, float), 'ct' (Closetime, datetime).
            max_size (int, optional): The maximum number of candles to keep in memory. Defaults to 100.
            valid_delay (timedelta, optional): The maximum allowed delay for considering a candle valid.
                Defaults to 20 seconds.
            *args: Variable length argument list for additional parameters.
            **kwargs: Arbitrary keyword arguments for additional parameters.

        The on_candles callback is a required parameter to ensure that all
        implementations have a mechanism to handle new candle data.
        """
        pass

    @abstractmethod
    def run_forever(self) -> None:
        """
        Run the candle fetcher indefinitely.

        This method should implement the main loop for fetching candles,
        handling any necessary reconnections or error recovery. It should
        continue running until explicitly stopped.

        During its operation, this method should call the on_candles callback
        whenever new data is available, passing a copy of the internal cache
        as a dictionary.
        """
        pass

    @abstractmethod
    def async_run_forever(self) -> None:
        """
        Asynchronously run the candle fetcher indefinitely.

        This method should provide an asynchronous interface for running
        the candle fetcher. It should be compatible with async/await syntax
        and should continue running until explicitly stopped.

        Similar to run_forever, this method should call the on_candles callback
        with a copy of the internal cache whenever new data is available.
        """
        pass

    @abstractmethod
    def stop(self) -> None:
        """
        Stop the candle fetcher.

        This method should implement the logic to stop the candle fetcher,
        including cleaning up any resources, closing connections, and
        ensuring a graceful shutdown of the fetching process.
        """
        pass
