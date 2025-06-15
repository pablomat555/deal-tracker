# exchanges.py
import ccxt

supported_exchanges = {
    "binance": ccxt.binance(),
    "bybit": ccxt.bybit(),
}
