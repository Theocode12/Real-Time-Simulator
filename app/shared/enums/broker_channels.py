from __future__ import annotations

from enum import StrEnum, auto


class BrokerChannels(StrEnum):
    CONTROLS = auto()
    SCORES_UPDATE = auto()
    COMMENTARY = auto()
