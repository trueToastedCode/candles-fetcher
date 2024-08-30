from websocket import WebSocketApp
from binance.client import Client

from candles_fetcher.binance_candles_fetcher import build_binance_candles_fetcher

BinanceCandlesFetcher = build_binance_candles_fetcher(WebSocketApp, Client)
