from setuptools import setup, find_packages

setup(
    name='candles_fetcher',
    version='0.1.0',
    description='Retrieve live market candle data with a periodic callback',
    author='trueToastedCode',
    packages=find_packages(),
    install_requires=['python-binance', 'websocket-client', 'pandas', 'awaitable']
)
