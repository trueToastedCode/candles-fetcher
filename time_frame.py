from enum import Enum, auto

class TimeFrame(Enum):
    ONE_SEC      = auto()
    ONE_MIN      = auto()
    THREE_MIN    = auto()
    FIVE_MIN     = auto()
    FIFTEEN_MIN  = auto()
    THIRTY_MIN   = auto()
    ONE_HOUR     = auto()
    TWO_HOURS    = auto()
    FOUR_HOURS   = auto()
    SIX_HOURS    = auto()
    EIGHT_HOURS  = auto()
    TWELVE_HOURS = auto()
    ONE_DAY      = auto()
    THREE_DAYS   = auto()
    ONE_WEEK     = auto()
    ONE_MONTH    = auto()
