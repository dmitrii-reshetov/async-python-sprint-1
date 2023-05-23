from abc import ABC


class ExpectedException(ABC, Exception):
    pass


class DataFetchingException(ExpectedException):
    pass


class DataCalculationException(ExpectedException):
    pass


class DataAggregationException(ExpectedException):
    pass
