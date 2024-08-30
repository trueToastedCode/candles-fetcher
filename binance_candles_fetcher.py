import time
from typing import Callable
from datetime import datetime, timedelta
import pandas as pd
import json
import traceback
from awaitable import awaitable

from time_frame import TimeFrame
from candles_fetcher_contract import CandlesFetcherContract

def build_binance_candles_fetcher(WebSocketApp, Client):
    """
    Factory function to build a BinanceCandlesFetcher class with injected dependencies.

    Args:
        WebSocketApp: WebSocket application class from a WebSocket library.
        Client: Binance API client class.

    Returns:
        BinanceCandlesFetcher: A class for fetching and managing candle data from Binance.
    """

    class BinanceCandlesFetcher(CandlesFetcherContract):
        """
        A class for fetching and managing candle data from Binance.

        This class implements the CandlesFetcherContract and provides functionality to:
        - Fetch historical candle data
        - Maintain a live WebSocket connection for real-time updates
        - Manage and update a DataFrame of candle data
        - Trigger callbacks when new data is available, passing a copy of the internal cache
        - Handles errors such as disconnections
        """

        __DF_COLUMNS = ['Opentime', 'Open', 'High', 'Low', 'Close', 'Closetime']

        def __init__(
            self,
            symbol     : str,
            time_frame : TimeFrame,
            on_candles : Callable,
            max_size   : int        = 100,
            valid_delay: timedelta  = timedelta(seconds=20)
        ):
            """
            Initialize the BinanceCandlesFetcher.

            Args:
                symbol (str): The trading pair symbol (e.g., 'BTCUSDT').
                time_frame (TimeFrame): The time frame for the candles.
                on_candles (Callable[[pd.DataFrame], None]): Callback function to be called with new candle data.
                    This function will receive a copy of the internal cache as a pandas DataFrame.
                max_size (int, optional): Maximum number of candles to keep in memory. Defaults to 100.
                valid_delay (timedelta, optional): Maximum allowed delay for considering data as valid. Defaults to 20 seconds.

            Raises:
                ValueError: If any of the input parameters are invalid.
            """
            if not (isinstance(symbol, str) and symbol):
                raise ValueError
            self.__symbol = symbol

            if not isinstance(time_frame, TimeFrame) or time_frame.value < time_frame.ONE_MIN.value:
                raise ValueError
            self.__time_frame = time_frame

            if not isinstance(on_candles, Callable):
                raise ValueError
            self.__on_candles = on_candles

            if not isinstance(max_size, int) or max_size < 1:
                raise ValueError
            self.__max_size = max_size

            if not isinstance(valid_delay, timedelta) or valid_delay == timedelta(0):
                raise ValueError
            self.__valid_delay = valid_delay

            self.__client                  = Client()
            self.__ws                      = None
            self.__df                      = None
            self.__initial_df              = None
            self.__is_initial_df_merged    = False
            self.__last_callback_open_time = None
            self.__keep_running            = False

        @property
        def client(self) -> Client:
            return self.__client

        @property
        def symbol(self) -> str:
            return self.__symbol

        @property
        def last_callback_open_time(self) -> datetime:
            return self.__last_callback_open_time

        @property
        def valid_delay(self) -> timedelta:
            return self.__valid_delay

        @property
        def time_frame(self) -> TimeFrame:
            return self.__time_frame

        @property
        def max_size(self) -> int:
            return self.__max_size

        @property
        def initial_df(self) -> pd.DataFrame:
            return self.__initial_df

        @property
        def df(self) -> pd.DataFrame:
            return self.__df

        @property
        def ws(self) -> WebSocketApp:
            return self.__ws

        @property
        def is_initial_df_merged(self) -> bool:
            return self.__is_initial_df_merged

        @property
        def on_candles(self) -> Callable:
            return self.__on_candles

        @property
        def keep_running(self) -> bool:
            return self.__keep_running

        def get_native_client_time_frame(self):
            """
            Get the native Binance client time frame string for the current time frame.

            Returns:
                str: Binance client time frame string.

            Raises:
                IndexError: If the current time frame is not supported.
            """
            idx = self.time_frame.value - TimeFrame.ONE_SEC.value - 1
            if idx < 0:
                raise IndexError
            return [
                Client.KLINE_INTERVAL_1MINUTE,
                Client.KLINE_INTERVAL_3MINUTE,
                Client.KLINE_INTERVAL_5MINUTE,
                Client.KLINE_INTERVAL_15MINUTE,
                Client.KLINE_INTERVAL_30MINUTE,
                Client.KLINE_INTERVAL_1HOUR,
                Client.KLINE_INTERVAL_2HOUR,
                Client.KLINE_INTERVAL_4HOUR,
                Client.KLINE_INTERVAL_6HOUR,
                Client.KLINE_INTERVAL_8HOUR,
                Client.KLINE_INTERVAL_12HOUR,
                Client.KLINE_INTERVAL_1DAY,
                Client.KLINE_INTERVAL_3DAY,
                Client.KLINE_INTERVAL_1WEEK,
                Client.KLINE_INTERVAL_1MONTH
            ][idx]

        def get_native_ws_timeframe(self):
            """
            Get the native WebSocket time frame string for the current time frame.

            Returns:
                str: WebSocket time frame string.

            Raises:
                IndexError: If the current time frame is not supported.
            """
            idx = self.time_frame.value - TimeFrame.ONE_SEC.value - 1
            if idx < 0:
                raise IndexError
            return [
                '1m',
                '3m',
                '5m',
                '15m',
                '30m',
                '1h',
                '2h',
                '4h',
                '6h',
                '8h',
                '12h',
                '1d',
                '3d',
                '1w',
                '1M'
            ][idx]

        def truncate_df(self, df: pd.DataFrame) -> pd.DataFrame:
            """
            Truncate the DataFrame to the maximum size.

            Args:
                df (pd.DataFrame): The DataFrame to truncate.

            Returns:
                pd.DataFrame: Truncated DataFrame.
            """
            if len(df.index) > self.max_size:
                return df.tail(self.max_size).reset_index(drop=True)
            return df

        def does_df_need_callback(self, df: pd.DataFrame) -> bool:
            """
            Check if the DataFrame needs a callback based on the last callback time and data validity.

            Args:
                df (pd.DataFrame): The DataFrame to check.

            Returns:
                bool: True if a callback is needed, False otherwise.
            """
            return (
                (
                    self.last_callback_open_time is None
                    or self.last_callback_open_time < df.Opentime.iloc[-1]
                )
                and datetime.utcnow() - df.Closetime.iloc[-1] <= self.valid_delay
            )

        def merge_initial_history_with_ws_updates(self):
            """
            Merge the initial historical data with WebSocket updates.

            This method combines the initial historical data fetched from the REST API
            with the real-time updates received via WebSocket.
            """
            # find first index in df, that includes new data over the history
            if self.df is None:
                first_valid_idx = None
            else:
                first_valid_idx = self.df[
                    self.initial_df.Opentime.iloc[-1] < self.df.Opentime
                ].first_valid_index()

            # if df doesn't have new data, use history entirely,
            # otherwise concat newer data
            if first_valid_idx is None:
                self.__df = self.initial_df
            else:
                self.__df = pd.concat(
                    [self.initial_df, self.df[first_valid_idx:]],
                    ignore_index=True
                )
            # mark merge as completed
            self.__initial_df           = None
            self.__is_initial_df_merged = True

        def on_open(self, ws: WebSocketApp):
            """
            Callback method called when the WebSocket connection is opened.

            This method subscribes to the candle events for the specified symbol and time frame.

            Args:
                ws (WebSocketApp): The WebSocket application instance.
            """
            # subscribe to candle events
            try:
                ws.send(json.dumps({
                    'method': 'SUBSCRIBE',
                    'params': [
                        f'{self.symbol.lower()}@kline_{self.get_native_ws_timeframe()}'
                    ],
                    'id': 1
                }))
            except Exception:
                # there should be no error here, therefore any exception here is critical
                # stop the websocket, possibly let it restart the callback loop
                # note: raising an exception here doesn't stop ws.run_forever
                print(traceback.format_exc())
                try:
                    self.ws.close()
                except:
                    pass

        def on_message(self, ws: WebSocketApp, message: str):
            """
            Callback method called when a message is received on the WebSocket.

            This method processes the incoming candle data, updates the internal DataFrame,
            and triggers the on_candles callback with a copy of the updated data if necessary.

            Args:
                ws (WebSocketApp): The WebSocket application instance.
                message (str): The received message as a JSON string.
            """
            try:
                message = json.loads(message)

                # check if it's a task result
                if 'result' in message:
                    # validate task succeeded
                    if message['result']:
                        raise RuntimeError(f'A websocket task failed with result {message["result"]}')
                    return

                # ignore unclosed candle events
                if not message['k']['x']:
                    return

                # parse candle
                update_df = pd.DataFrame(
                    {
                        'Opentime' : datetime.utcfromtimestamp(message['k']['t'] / 1000),
                        'Open'     : float(message['k']['o']),
                        'High'     : float(message['k']['h']),
                        'Low'      : float(message['k']['l']),
                        'Close'    : float(message['k']['c']),
                        'Closetime': datetime.utcfromtimestamp(message['k']['T'] / 1000),
                    },
                    index=[0]
                )

                # ensure new candle is an actual update for the cache
                if self.df is not None and update_df.Opentime[0] <= self.df.Opentime.iloc[-1]:
                    return

                # concat to df
                self.__df = pd.concat([self.df, update_df], ignore_index=True)

                # nothing to do if initial history not available and not already merged
                if not self.is_initial_df_merged and self.initial_df is None:
                    return

                # merge initial history if available and not merged already
                if not self.is_initial_df_merged and self.initial_df is not None:
                    self.merge_initial_history_with_ws_updates()

                # keep in limit
                self.__df = self.truncate_df(self.df)

                # callback if necessary
                if self.does_df_need_callback(self.df):
                    self.__last_callback_open_time = self.df.Opentime.iloc[-1]
                    self.on_candles(self.df.copy())
            except Exception:
                # there should be no error here, therefore any exception here is critical
                # stop the websocket, possibly let it restart the callback loop
                # note: raising an exception here doesn't stop ws.run_forever
                print(traceback.format_exc())
                try:
                    self.ws.close()
                except:
                    pass

        def run(self) -> None:
            """
            Run the candle fetcher.

            This method initializes the WebSocket, fetches initial historical data,
            starts the WebSocket connection for real-time updates, and ensures that
            the on_candles callback is called with a copy of the internal cache when new data arrives.
            """
            # reset state variables
            self.__df                   = None
            self.__initial_df           = None
            self.__is_initial_df_merged = False
            # leave __last_callback_open_time as is for reconnections

            # init web socket
            self.__ws = WebSocketApp(
                'wss://stream.binance.com:9443/ws',
                on_open=self.on_open,
                on_message=self.on_message
            )

            # fetch initial historical candles
            data = self.client.get_historical_klines(
                symbol=self.symbol.upper(),
                interval=self.get_native_client_time_frame()
            )

            # parse data
            initial_history_df = pd.DataFrame(
                map(
                    lambda x: [
                        datetime.utcfromtimestamp(x[0] / 1000),  # Open time
                        float(x[1]),                             # Open
                        float(x[2]),                             # High
                        float(x[3]),                             # Low
                        float(x[4]),                             # Close
                        datetime.utcfromtimestamp(x[6] / 1000),  # Close time
                    ],
                    data
                ),
                columns=self.__DF_COLUMNS
            )

            # drop unclosed candle if necessary
            if initial_history_df.Closetime.iloc[-1] > datetime.utcnow():
                initial_history_df = initial_history_df.drop(initial_history_df.index[-1])

            # keep in limit
            initial_history_df = self.truncate_df(initial_history_df)

            # callback if necessary
            if self.does_df_need_callback(initial_history_df):
                self.__last_callback_open_time = initial_history_df.Opentime.iloc[-1]
                self.on_candles(initial_history_df.copy())

            # set initial history to be merged with websocket updates
            self.__initial_df = initial_history_df

            # keep websocket open until it disconnects
            self.ws.run_forever()

        def run_forever(self):
            """
            Run the candle fetcher indefinitely.

            This method keeps the candle fetcher running, handling disconnections and errors.
            """
            if self.keep_running:
                raise ValueError
            self.__keep_running = True
            while self.keep_running:
                try:
                    self.run()
                except:
                    # on_message and on_open not caught here
                    # errors there will just stop the websocket
                    time.sleep(1)

        @awaitable
        def async_run_forever(self):
            """
            Asynchronously run the candle fetcher indefinitely.

            This method is a wrapper around run_forever() that can be awaited for.
            """
            self.run_forever()

        def stop(self):
            """
            Stop the candle fetcher.

            This method stops the indefinite running of the candle fetcher and closes the WebSocket connection.
            """
            self.__keep_running = False
            try:
                self.ws.close()
            except:
                pass

    return BinanceCandlesFetcher
