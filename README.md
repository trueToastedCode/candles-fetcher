# Candles fetcher
Retrieve live market candle data with a periodic callback.
## Usage example
```
from candles_fetcher import BinanceCandlesFetcher, TimeFrame

def on_candles(candles):
    print(candles)

if __name__ == '__main__':
    cf = BinanceCandlesFetcher('btcusdt', TimeFrame.ONE_MIN, on_candles)
    cf.run_forever()
```
