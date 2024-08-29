import unittest
from unittest.mock import patch, PropertyMock
import pandas as pd
from binance.client import Client
from websocket import WebSocketApp
from datetime import datetime, timedelta
import calendar
import json

from binance_candles_fetcher import build_binance_candles_fetcher
from time_frame import TimeFrame

class TestBinanceCandlesFetcher(unittest.TestCase):
    """
    Test suite for the BinanceCandlesFetcher class.

    This class contains unit tests for various methods and functionalities
    of the BinanceCandlesFetcher, including initialization, data processing,
    and WebSocket interactions.
    """

    __MOCK_LAST_CALLBACK_OPEN_TIME_PATH = 'test_binance_candles_fetcher.TestBinanceCandlesFetcher.BinanceCandlesFetcher.last_callback_open_time'
    __MOCK_TIME_FRAME_PATH              = 'test_binance_candles_fetcher.TestBinanceCandlesFetcher.BinanceCandlesFetcher.time_frame'

    __DUMMY_ARGS = 'btcusdt', TimeFrame.ONE_MIN, lambda _: None, 100, timedelta(seconds=20)

    class MockWebSocketApp(WebSocketApp):
        """Mock WebSocketApp for testing purposes."""
        def __init__(self, *args, **kwargs):
            self.on_open = kwargs.get('on_open')
            self.on_message = kwargs.get('on_message')

    class MockClient(Client):
        """Mock Client for testing purposes."""
        def __init__(self, *args, **kwargs):
            pass

        def close_connection(self, *args, **kwargs):
            pass

    BinanceCandlesFetcher = build_binance_candles_fetcher(
        MockWebSocketApp,
        MockClient
    )

    def setUp(self):
        """
        Set up the test environment before each test method is run.

        This method loads historical chart data from a JSON file and prepares
        it for use in the tests, including splitting it into initial history
        and WebSocket updates.
        """
        # load historical chart data
        # [(Open time, Open, High, Low, Close, Volume, Close time, Quote asset volume, Number of trades, Taker buy base asset volume, Taker buy quote asset volume, Ignore), ...]
        with open('historical.json', 'r') as f:
            data = json.load(f)

        # Split a part for the initial history
        self.__initial_history = data[:110]

        #  Process the rest into the format emitted by the websocket
        self.__ws_history = list(map(
            lambda x: {
                'k': {
                    't': x[0],  # Open time
                    'T': x[6],  # Close time
                    'o': x[1],  # Open
                    'h': x[2],  # High
                    'c': x[3],  # Low
                    'l': x[4],  # Close
                    'x': True   # Candle closed
                }
            },
            data[110:]
        ))

        # Create a dummy candles fetcher for testing some components
        self.__dummy_cf = self.BinanceCandlesFetcher(*self.__DUMMY_ARGS)

    def test_init_raises(self):
        """
        Test that the BinanceCandlesFetcher.__init__() method raises
        ValueError for invalid input parameters.
        """
        # invalid symbol
        with self.subTest():
            self.assertRaises(ValueError, self.BinanceCandlesFetcher, '', TimeFrame.ONE_MIN, lambda _: None)
        # invalid timeframe
        with self.subTest():
            self.assertRaises(ValueError, self.BinanceCandlesFetcher, 'btcusdt', TimeFrame.ONE_SEC, lambda _: None)
        # invalid callback function
        with self.subTest():
            self.assertRaises(ValueError, self.BinanceCandlesFetcher, 'btcusdt', TimeFrame.ONE_MIN, None)
        # invalid max cache size
        with self.subTest():
            self.assertRaises(ValueError, self.BinanceCandlesFetcher, 'btcusdt', TimeFrame.ONE_MIN, lambda _: None, 0)
        # invalid delay
        with self.subTest():
            self.assertRaises(ValueError, self.BinanceCandlesFetcher, 'btcusdt', TimeFrame.ONE_MIN, lambda _: None, 1, timedelta(0))

    def test_truncate_df(self):
        """
        Test the truncate_df() method of BinanceCandlesFetcher.

        Ensures that the method correctly truncates a DataFrame to the
        specified maximum cache size.
        """
        df = pd.DataFrame(index=range(110))
        df = self.__dummy_cf.truncate_df(df)
        self.assertEqual(df.index[0], 0, "First index should be 0 after truncation")
        self.assertEqual(df.index[-1], 99, "Last index should be 99 after truncation to 100 rows")

    def test_does_df_need_callback(self):
        """
        Test the does_df_need_callback() method of BinanceCandlesFetcher.

        This method tests various scenarios to determine if a callback
        is needed based on the current time and the last callback time.
        """
        now = datetime.utcnow()
        df = pd.DataFrame(
            [
                [
                    now - timedelta(minutes=2),  # Opentime
                    now                          # Closetime
                ]
            ],
            columns=['Opentime', 'Closetime']
        )

        # callback already called
        with self.subTest():
            with patch(self.__MOCK_LAST_CALLBACK_OPEN_TIME_PATH, new_callable=PropertyMock) as mock_last_callback_open_time:
                # now greater than last df.opentime
                # therefore last df.opentime has been called for already
                mock_last_callback_open_time.return_value = now
                self.assertFalse(self.__dummy_cf.does_df_need_callback(df), "Callback should not be needed when last_callback_open_time is current")

        # closetime still valid
        with self.subTest():
            with patch(self.__MOCK_LAST_CALLBACK_OPEN_TIME_PATH, new_callable=PropertyMock) as mock_last_callback_open_time:
                # last callback before current opentime
                # and current close still valid
                mock_last_callback_open_time.return_value = now - timedelta(minutes=4)
                self.assertTrue(self.__dummy_cf.does_df_need_callback(df), "Callback should be needed when last_callback_open_time is older than current opentime")

        # closetime beyond delay
        with self.subTest():
            with patch(self.__MOCK_LAST_CALLBACK_OPEN_TIME_PATH, new_callable=PropertyMock) as mock_last_callback_open_time:
                # now - timedelta(minutes=1) beyond valid delay
                # therefore no callback
                mock_last_callback_open_time.return_value = now - timedelta(minutes=1)
                self.assertFalse(self.__dummy_cf.does_df_need_callback(df), "Callback should not be needed when closetime is beyond delay")

    def test_get_native_client_timeframe(self):
        """
        Test the get_native_client_time_frame() method of BinanceCandlesFetcher.

        This method tests the conversion of TimeFrame enums to their
        corresponding Binance Client interval strings.
        """
        # Test all intervals
        intervals = [
            (TimeFrame.ONE_MIN, Client.KLINE_INTERVAL_1MINUTE),
            (TimeFrame.THREE_MIN, Client.KLINE_INTERVAL_3MINUTE),
            (TimeFrame.FIVE_MIN, Client.KLINE_INTERVAL_5MINUTE),
            (TimeFrame.FIFTEEN_MIN, Client.KLINE_INTERVAL_15MINUTE),
            (TimeFrame.THIRTY_MIN, Client.KLINE_INTERVAL_30MINUTE),
            (TimeFrame.ONE_HOUR, Client.KLINE_INTERVAL_1HOUR),
            (TimeFrame.TWO_HOURS, Client.KLINE_INTERVAL_2HOUR),
            (TimeFrame.FOUR_HOURS, Client.KLINE_INTERVAL_4HOUR),
            (TimeFrame.SIX_HOURS, Client.KLINE_INTERVAL_6HOUR),
            (TimeFrame.EIGHT_HOURS, Client.KLINE_INTERVAL_8HOUR),
            (TimeFrame.TWELVE_HOURS, Client.KLINE_INTERVAL_12HOUR),
            (TimeFrame.ONE_DAY, Client.KLINE_INTERVAL_1DAY),
            (TimeFrame.THREE_DAYS, Client.KLINE_INTERVAL_3DAY),
            (TimeFrame.ONE_WEEK, Client.KLINE_INTERVAL_1WEEK),
            (TimeFrame.ONE_MONTH, Client.KLINE_INTERVAL_1MONTH)
        ]

        for time_frame, expected_client_interval in intervals:
            with self.subTest(time_frame=time_frame):
                with patch(self.__MOCK_TIME_FRAME_PATH, new_callable=PropertyMock) as mock_time_frame:
                    mock_time_frame.return_value = time_frame
                    result = self.__dummy_cf.get_native_client_time_frame()
                    self.assertEqual(result, expected_client_interval, f"Incorrect client interval for {time_frame}")

        # Test for ONE_SEC (which should raise an IndexError)
        with self.subTest():
            with patch(self.__MOCK_TIME_FRAME_PATH, new_callable=PropertyMock) as mock_time_frame:
                mock_time_frame.return_value = TimeFrame.ONE_SEC
                self.assertRaises(IndexError, self.__dummy_cf.get_native_client_time_frame)

    def test_get_native_ws_timeframe(self):
        """
        Test the get_native_ws_timeframe() method of BinanceCandlesFetcher.

        This method tests the conversion of TimeFrame enums to their
        corresponding WebSocket interval strings.
        """
        # Test all intervals
        intervals = [
            (TimeFrame.ONE_MIN, '1m'),
            (TimeFrame.THREE_MIN, '3m'),
            (TimeFrame.FIVE_MIN, '5m'),
            (TimeFrame.FIFTEEN_MIN, '15m'),
            (TimeFrame.THIRTY_MIN, '30m'),
            (TimeFrame.ONE_HOUR, '1h'),
            (TimeFrame.TWO_HOURS, '2h'),
            (TimeFrame.FOUR_HOURS, '4h'),
            (TimeFrame.SIX_HOURS, '6h'),
            (TimeFrame.EIGHT_HOURS, '8h'),
            (TimeFrame.TWELVE_HOURS, '12h'),
            (TimeFrame.ONE_DAY, '1d'),
            (TimeFrame.THREE_DAYS, '3d'),
            (TimeFrame.ONE_WEEK, '1w'),
            (TimeFrame.ONE_MONTH, '1M')
        ]

        for time_frame, expected_client_interval in intervals:
            with self.subTest(time_frame=time_frame):
                with patch(self.__MOCK_TIME_FRAME_PATH, new_callable=PropertyMock) as mock_time_frame:
                    mock_time_frame.return_value = time_frame
                    result = self.__dummy_cf.get_native_ws_timeframe()
                    self.assertEqual(result, expected_client_interval, f"Incorrect WebSocket interval for {time_frame}")

        # Test for ONE_SEC (which should raise an IndexError)
        with self.subTest():
            with patch(self.__MOCK_TIME_FRAME_PATH, new_callable=PropertyMock) as mock_time_frame:
                mock_time_frame.return_value = TimeFrame.ONE_SEC
                self.assertRaises(IndexError, self.__dummy_cf.get_native_ws_timeframe)

    @staticmethod
    def on_candles(df):
        """Mock callback function for testing."""
        pass

    @patch('test_binance_candles_fetcher.TestBinanceCandlesFetcher.on_candles')
    @patch('test_binance_candles_fetcher.TestBinanceCandlesFetcher.MockWebSocketApp.run_forever')
    @patch('test_binance_candles_fetcher.TestBinanceCandlesFetcher.MockClient.get_historical_klines')
    def test_run(self, mock_get_historical_klines, mock_run_forever, mock_on_candles):
        """
        Test the run() method of BinanceCandlesFetcher.

        This method tests the initialization process, including fetching
        historical data, setting up the WebSocket, and triggering the
        initial callback.
        """
        # manipulate history to trigger some if statements
        initial_history = self.__initial_history.copy()
        now = datetime.utcnow()
        now -= timedelta(microseconds=now.microsecond)
        # simulate latest candle to be unclosed
        initial_history[-1][6] = calendar.timegm((now + timedelta(minutes=1)).utctimetuple()) * 1000
        # simulate candle before to be still valid
        initial_history[-2][6] = calendar.timegm(now.utctimetuple()) * 1000
        mock_get_historical_klines.return_value = initial_history

        # instantiate fetcher and call run method
        cf = self.BinanceCandlesFetcher('btcusdt', TimeFrame.ONE_MIN, self.on_candles)
        with patch('test_binance_candles_fetcher.TestBinanceCandlesFetcher.BinanceCandlesFetcher.truncate_df', side_effect=cf.truncate_df) as mock_truncate_df:
            cf.run()

            # validate state
            mock_get_historical_klines.assert_called_once_with(symbol='BTCUSDT', interval=Client.KLINE_INTERVAL_1MINUTE)
            mock_run_forever.assert_called_once()
            mock_on_candles.assert_called_once()
            mock_truncate_df.assert_called_once()
            callback_df = mock_on_candles.call_args[0][0]
            self.assertEqual(len(callback_df.index), 100, "Callback DataFrame should have 100 rows")
            self.assertEqual(callback_df.Closetime.iloc[-1], now, "Last closetime should match current time")
            self.assertEqual(cf.last_callback_open_time, callback_df.Opentime.iloc[-1], "last_callback_open_time should match last opentime in DataFrame")
            self.assertIsNotNone(cf.initial_df, "initial_df should be set")
            self.assertIsNone(cf.df, "df should be None after initial callback")
            self.assertFalse(cf.is_initial_df_merged, "is_initial_df_merged should be False")
            self.assertIsNotNone(cf.ws, "WebSocket should be initialized")

    @patch('test_binance_candles_fetcher.TestBinanceCandlesFetcher.MockWebSocketApp.send')
    @patch('test_binance_candles_fetcher.TestBinanceCandlesFetcher.MockWebSocketApp.run_forever')
    @patch('test_binance_candles_fetcher.TestBinanceCandlesFetcher.MockClient.get_historical_klines')
    def test_on_open(self, mock_get_historical_klines, mock_run_forever, mock_send):
        """
        Test the on_open() method of BinanceCandlesFetcher.

        This method tests the WebSocket connection opening process,
        including sending the subscription message.
        """
        mock_get_historical_klines.return_value = self.__initial_history.copy()
        cf = self.BinanceCandlesFetcher(*self.__DUMMY_ARGS)
        cf.run()
        cf.ws.on_open(cf.ws)
        mock_send.assert_called_once_with('{"method": "SUBSCRIBE", "params": ["btcusdt@kline_1m"], "id": 1}')

    def test_merge_initial_history_with_ws_updates_and_overlap(self):
        # setup some dummy dataframes
        fmt = '%Y-%m-%d %H:%M'

        initial_df = pd.DataFrame(
            [
                '2024-01-01 00:00',
                '2024-01-01 01:00',
                '2024-01-01 02:00'
            ],
            columns=['Opentime']
        )
        initial_df['Opentime'] = pd.to_datetime(initial_df.Opentime, format=fmt)

        df = pd.DataFrame(
            [
                '2024-01-01 01:00',
                '2024-01-01 02:00',
                '2024-01-01 03:00'
            ],
            columns=['Opentime']
        )
        df['Opentime'] = pd.to_datetime(df.Opentime, format=fmt)

        # merge
        cf = self.BinanceCandlesFetcher(*self.__DUMMY_ARGS)
        with patch('test_binance_candles_fetcher.TestBinanceCandlesFetcher.BinanceCandlesFetcher.df', new_callable=PropertyMock) as mock_df:
            mock_df.return_value = df
            with patch('test_binance_candles_fetcher.TestBinanceCandlesFetcher.BinanceCandlesFetcher.initial_df', new_callable=PropertyMock) as mock_initial_df:
                mock_initial_df.return_value = initial_df
                cf.merge_initial_history_with_ws_updates()

        # validate state
        self.assertIsNone(cf.initial_df, 'Initial df should be none after merge')
        self.assertIsInstance(cf.df, pd.DataFrame, 'df should be a DataFrame instance')
        self.assertEqual(len(cf.df.index), 4, 'Merged length should equal 4')
        self.assertEqual(cf.df.Opentime[0], initial_df.Opentime[0], '0. row from initial df')
        self.assertEqual(cf.df.Opentime[1], initial_df.Opentime[1], '1. row from initial df')
        self.assertEqual(cf.df.Opentime[2], initial_df.Opentime[2], '2. row from initial df')
        self.assertEqual(cf.df.Opentime[3], df.Opentime[2], '3. row from ws update')

    def test_merge_initial_history_with_ws_updates_and_no_overlap(self):
        # setup some dummy dataframes
        fmt = '%Y-%m-%d %H:%M'

        initial_df = pd.DataFrame(
            [
                '2024-01-01 01:00',
                '2024-01-01 02:00',
                '2024-01-01 03:00'
            ],
            columns=['Opentime']
        )
        initial_df['Opentime'] = pd.to_datetime(initial_df.Opentime, format=fmt)

        df = pd.DataFrame(
            [
                '2024-01-01 02:00',
            ],
            columns=['Opentime']
        )
        df['Opentime'] = pd.to_datetime(df.Opentime, format=fmt)

        # merge
        cf = self.BinanceCandlesFetcher(*self.__DUMMY_ARGS)
        with patch('test_binance_candles_fetcher.TestBinanceCandlesFetcher.BinanceCandlesFetcher.df', new_callable=PropertyMock) as mock_df:
            mock_df.return_value = df
            with patch('test_binance_candles_fetcher.TestBinanceCandlesFetcher.BinanceCandlesFetcher.initial_df', new_callable=PropertyMock) as mock_initial_df:
                mock_initial_df.return_value = initial_df
                cf.merge_initial_history_with_ws_updates()

        # validate state
        self.assertIsNone(cf.initial_df, 'Initial df should be none after merge')
        self.assertIsInstance(cf.df, pd.DataFrame, 'df should be a DataFrame instance')
        self.assertEqual(len(cf.df.index), 3, 'Merged length should equal 4')
        self.assertEqual(cf.df.Opentime[0], initial_df.Opentime[0], '0. row from initial df')
        self.assertEqual(cf.df.Opentime[1], initial_df.Opentime[1], '1. row from initial df')
        self.assertEqual(cf.df.Opentime[2], initial_df.Opentime[2], '2. row from initial df')

if __name__ == '__main__':
    unittest.main()
