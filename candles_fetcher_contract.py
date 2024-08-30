from abc import ABC, abstractmethod
from typing import Callable

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
    def __init__(self, on_candles: Callable, *args, **kwargs):
        """
        Initialize the candle fetcher.

        Args:
            on_candles (Callable[[pd.DataFrame], None]): Callback function to be called with new candle data.
                This function should accept a single argument 'df', which is a pandas DataFrame
                containing the columns 'Opentime', 'Open', 'High', 'Low', 'Close', 'Closetime'.
                The DataFrame passed to this callback must be a copy of the internal cache.
            *args: Variable length argument list for additional parameters.
            **kwargs: Arbitrary keyword arguments for additional parameters.

        The on_candles callback is a required parameter to ensure that all
        implementations have a mechanism to handle new candle data.
        """
        pass

    @abstractmethod
    def run_forever(self):
        """
        Run the candle fetcher indefinitely.

        This method should implement the main loop for fetching candles,
        handling any necessary reconnections or error recovery. It should
        continue running until explicitly stopped.

        During its operation, this method should call the on_candles callback
        whenever new data is available, passing a copy of the internal cache
        as a pandas DataFrame.
        """
        pass

    @abstractmethod
    def async_run_forever(self):
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
    def stop(self):
        """
        Stop the candle fetcher.

        This method should implement the logic to stop the candle fetcher,
        including cleaning up any resources, closing connections, and
        ensuring a graceful shutdown of the fetching process.
        """
        pass
