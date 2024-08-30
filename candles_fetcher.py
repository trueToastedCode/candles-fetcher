from websocket import WebSocketApp
from binance.client import Client

from binance_candles_fetcher import build_binance_candles_fetcher

BinanceCandlesFetcher = build_binance_candles_fetcher(WebSocketApp, Client)
