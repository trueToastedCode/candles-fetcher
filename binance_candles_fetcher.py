from typing import Callable
from datetime import datetime, timedelta
import pandas as pd
import json
import traceback

from time_frame import TimeFrame

def build_binance_candles_fetcher(WebSocketApp, Client):
    class BinanceCandlesFetcher:
        __DF_COLUMNS = ['Opentime', 'Open', 'High', 'Low', 'Close', 'Closetime']

        def __init__(
            self,
            symbol     : str,
            time_frame : TimeFrame,
            on_candles : Callable,
            max_size   : int        = 100,
            valid_delay: timedelta  = timedelta(seconds=20)
        ):
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

        def get_native_client_time_frame(self):
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
            idx = self.time_frame.value - TimeFrame.ONE_SEC.value - 1
            if idx < 0:
                raise IndexError
            return [
                '1min',
                '3min',
                '5min',
                '15min',
                '30min',
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
            if len(df.index) > self.max_size:
                return df.tail(self.max_size).reset_index(drop=True)
            return df

        def does_df_need_callback(self, df: pd.DataFrame) -> bool:
            return (
                (
                    self.last_callback_open_time is None
                    or self.last_callback_open_time < df.Opentime.iloc[-1]
                )
                and datetime.utcnow() - df.Closetime.iloc[-1] <= self.valid_delay
            )

        def on_open(self, ws: WebSocketApp):
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
                # force program to quit if something happens
                print(traceback.format_exc())
                exit(1)

        def on_message(self, ws: WebSocketApp, message: str):
            raise NotImplementedError

        def run(self) -> None:
            # reset state variables
            self.__df                    = None
            self.__initial_df            = None
            self.__is_initial_df_merged  = False
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
                self.__on_candles(initial_history_df.copy())

            # set initial history to be merged with websocket updates
            self.__initial_df = initial_history_df

            # keep websocket open until it disconnects
            self.ws.run_forever()

    return BinanceCandlesFetcher
