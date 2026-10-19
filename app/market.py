"""Simulation métier indépendante du client Kafka."""
import math
import random
import uuid
from datetime import datetime, timezone


SYMBOLS = (
    "AAPL",  # Apple
    "MSFT",  # Microsoft
    "NVDA",  # NVIDIA
    "AMZN",  # Amazon
    "GOOGL", # Alphabet
    "META",  # Meta Platforms
    "TSLA",  # Tesla
    "BRK.B", # Berkshire Hathaway
    "JPM",   # JPMorgan Chase
    "V",     # Visa
    "WMT",   # Walmart
    "LLY",   # Eli Lilly
    "AVGO",  # Broadcom
    "ORCL",  # Oracle
    "NFLX",  # Netflix
    "AMD",   # AMD
    "COST",  # Costco
    "MA",    # Mastercard
    "XOM",   # Exxon Mobil
    "KO",    # Coca-Cola
)


class Market:
    def __init__(self, seed=42):
        self.rng = random.Random(seed)
        self.prices = {
            symbol: 100.0 + 3 * i
            for i, symbol in enumerate(SYMBOLS)
        }
        self.sequence = 0

    def shock(self, symbol="AAPL", factor=1.08):
        if symbol not in self.prices:
            raise ValueError(f"Unknown symbol: {symbol}")
        self.prices[symbol] *= factor

    def next_trade(self):
        # Les 20 symboles passent successivement dans chaque cycle.
        symbol = SYMBOLS[self.sequence % len(SYMBOLS)]
        self.sequence += 1

        self.prices[symbol] = max(
            0.01,
            self.prices[symbol]
            * math.exp(self.rng.gauss(0, 0.00008)),
        )

        return {
            "trade_id": uuid.uuid4().hex,
            "symbol": symbol,
            "price": round(self.prices[symbol], 4),
            "quantity": self.rng.randint(1, 100),
            "timestamp": datetime.now(timezone.utc).isoformat(
                timespec="microseconds"
            ),
        }