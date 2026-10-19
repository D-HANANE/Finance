"""Simulation métier indépendante du client Kafka."""
import math
import random
import uuid
from datetime import datetime, timezone


SYMBOLS = (
    "AAPL",   # Apple
    "MSFT",   # Microsoft
    "AMZN",   # Amazon
    "GOOGL",  # Alphabet
    "META",   # Meta
    "NVDA",   # NVIDIA
    "TSLA",   # Tesla
    "NFLX",   # Netflix
    "ADBE",   # Adobe
    "CRM",    # Salesforce
    "ORCL",   # Oracle
    "IBM",    # IBM
    "INTC",   # Intel
    "AMD",    # AMD
    "JPM",    # JPMorgan Chase
    "BAC",    # Bank of America
    "V",      # Visa
    "MA",     # Mastercard
    "KO",     # Coca-Cola
    "WMT",    # Walmart
)


class Market:
    def __init__(self, seed=42):
        self.rng = random.Random(seed)
        self.prices = {symbol: 100.0 + 3 * i for i, symbol in enumerate(SYMBOLS)}
        self.sequence = 0

    def shock(self, symbol="AAPL", factor=1.08):
        self.prices[symbol] *= factor

    def next_trade(self):
        # Répartition uniforme : les 20 symboles passent dans chaque cycle.
        symbol = SYMBOLS[self.sequence % len(SYMBOLS)]
        self.sequence += 1
        self.prices[symbol] = max(
            0.01, self.prices[symbol] * math.exp(self.rng.gauss(0, 0.00008))
        )
        return {
            "trade_id": uuid.uuid4().hex,
            "symbol": symbol,
            "price": round(self.prices[symbol], 4),
            "volume": self.rng.randint(1, 100),
            "event_time": datetime.now(timezone.utc).isoformat(timespec="microseconds"),
        }
