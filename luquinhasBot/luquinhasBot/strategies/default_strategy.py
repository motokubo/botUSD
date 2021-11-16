import random
import sys
from datetime import datetime

from luquinhasBot.auto_trader import AutoTrader


class Strategy(AutoTrader):
    def initialize(self):
        super().initialize()
