# Candles fetcher
Retrieve live market candle data with a periodic callback.
## Usage example
```
from candles_fetcher import BinanceCandlesFetcher
from time_frame import TimeFrame

def on_candles(df):
    print(df)

if __name__ == '__main__':
    cf = BinanceCandlesFetcher('btcusdt', TimeFrame.ONE_MIN, on_candles)
    cf.run_forever()
```
