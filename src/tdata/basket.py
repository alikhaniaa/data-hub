"""Instrument basket tracked through Yahoo Finance.

Each row: (symbol, name, group, note)
"""
from __future__ import annotations

# ------------------------------------------------------------------ cross-asset context for crypto (basket "macro")
MACRO = [
    ("^GSPC", "S&P 500", "index", "US equity beta; BTC correlates with risk appetite"),
    ("^NDX", "Nasdaq 100", "index", "growth/tech beta"),
    ("^IXIC", "Nasdaq Composite", "index", ""),
    ("^DJI", "Dow Jones", "index", ""),
    ("^RUT", "Russell 2000", "index", "small caps, rate sensitive"),
    ("^VIX", "VIX", "vol", "equity fear gauge"),
    ("^TNX", "US 10y yield", "rates", "real-rate driver for crypto"),
    ("^FVX", "US 5y yield", "rates", ""),
    ("^IRX", "US 13w bill", "rates", "policy-rate proxy"),
    ("^TYX", "US 30y yield", "rates", ""),
    ("DX-Y.NYB", "US Dollar Index", "fx", "dollar liquidity; BTC usually inverse"),
    ("EURUSD=X", "EUR/USD", "fx", ""),
    ("USDJPY=X", "USD/JPY", "fx", "carry-trade unwind risk"),
    ("GBPUSD=X", "GBP/USD", "fx", ""),
    ("USDCNY=X", "USD/CNY", "fx", ""),
    ("USDCHF=X", "USD/CHF", "fx", ""),
    ("AUDUSD=X", "AUD/USD", "fx", "risk-on proxy"),
    ("USDCAD=X", "USD/CAD", "fx", ""),
    ("USDTRY=X", "USD/TRY", "fx", "lira: heavy crypto/stablecoin use"),
    ("USDINR=X", "USD/INR", "fx", ""),
    ("USDKRW=X", "USD/KRW", "fx", "Korean retail crypto demand"),
    ("USDBRL=X", "USD/BRL", "fx", ""),
    ("USDMXN=X", "USD/MXN", "fx", ""),
    ("GC=F", "Gold futures", "commodity", "store of value; BTC competitor narrative"),
    ("SI=F", "Silver futures", "commodity", ""),
    ("HG=F", "Copper futures", "commodity", "growth proxy"),
    ("CL=F", "WTI crude", "commodity", ""),
    ("BZ=F", "Brent crude", "commodity", ""),
    ("NG=F", "Natural gas (Henry Hub)", "commodity", ""),
    ("ZW=F", "Wheat", "commodity", ""),
    ("TLT", "20y+ Treasury ETF", "bond_etf", ""),
    ("IEF", "7-10y Treasury ETF", "bond_etf", ""),
    ("SHY", "1-3y Treasury ETF", "bond_etf", ""),
    ("HYG", "High yield credit ETF", "credit", "risk appetite"),
    ("LQD", "Investment grade credit ETF", "credit", ""),
    ("TIP", "TIPS ETF", "credit", "inflation expectations"),
    ("XLF", "Financials", "sector", ""), ("XLK", "Technology", "sector", ""), ("XLE", "Energy sector", "sector", ""),
    ("XLI", "Industrials", "sector", ""), ("XLU", "Utilities", "sector", ""), ("XLY", "Cons. discretionary", "sector", ""),
    ("XLP", "Cons. staples", "sector", ""), ("XLV", "Health care", "sector", ""),
    ("BTC-USD", "Bitcoin (Yahoo, long history)", "crypto_ref", ""),
    ("ETH-USD", "Ethereum (Yahoo)", "crypto_ref", ""),
    ("SOL-USD", "Solana (Yahoo)", "crypto_ref", ""),
    ("MSTR", "Strategy (BTC treasury)", "crypto_equity", ""),
    ("COIN", "Coinbase", "crypto_equity", ""),
    ("MARA", "Marathon Digital", "crypto_equity", "miner"), ("RIOT", "Riot Platforms", "crypto_equity", "miner"),
    ("CLSK", "CleanSpark", "crypto_equity", "miner"), ("IREN", "IREN", "crypto_equity", "miner/AI"),
    ("CIFR", "Cipher Mining", "crypto_equity", "miner"), ("WULF", "TeraWulf", "crypto_equity", "miner"),
    ("HUT", "Hut 8", "crypto_equity", "miner"), ("HOOD", "Robinhood", "crypto_equity", ""),
    ("CRCL", "Circle (USDC)", "crypto_equity", "stablecoin issuer"), ("GLXY", "Galaxy Digital", "crypto_equity", ""),
    ("IBIT", "iShares Bitcoin Trust", "crypto_etf", "spot BTC ETF flows"), ("FBTC", "Fidelity Wise Origin BTC", "crypto_etf", ""),
    ("GBTC", "Grayscale Bitcoin Trust", "crypto_etf", ""), ("ETHA", "iShares Ethereum Trust", "crypto_etf", ""),
    ("ETHE", "Grayscale Ethereum Trust", "crypto_etf", ""), ("BITO", "ProShares BTC futures ETF", "crypto_etf", ""),
]


def macro_symbols() -> list[str]:
    return list(dict.fromkeys(r[0] for r in MACRO))


def meta_rows():
    return [{"basket": "macro", "symbol": sym, "name": name, "group": grp, "note": note} for sym, name, grp, note in MACRO]
