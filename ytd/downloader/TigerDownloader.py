from ..SimpleSymbolDownloader import SymbolDownloader
from ..symbols.Generic import Generic
from time import sleep
from ..compat import text
import requests

class TigerDownloader(SymbolDownloader):
    def __init__(self):
        SymbolDownloader.__init__(self, "tiger")

    def _add_queries(self, prefix=''):
        elements = ['A','APPL','MSFT']
        for element in elements:
            if element not in self.queries:  # Avoid having duplicates in list
                self.queries.append(element)

    def nextRequest(self, insecure=False, pandantic=False):
        self._nextQuery()
        success = False
        retryCount = 0
        json = None
        # Eponential back-off algorithm
        # to attempt 5 more times sleeping 5, 25, 125, 625, 3125 seconds
        # respectively.
        maxRetries = 5
        while (success == False):
            try:
                json = self._fetch(insecure)
                success = True
            except (requests.HTTPError,
                    requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ReadTimeout,
                    requests.exceptions.ConnectionError) as ex:
                if retryCount < maxRetries:
                    attempt = retryCount + 1
                    sleepAmt = int(math.pow(5, attempt))
                    print("Retry attempt: " + str(attempt) + " of " + str(maxRetries) + "."
                                                                                        " Sleep period: " + str(
                        sleepAmt) + " seconds."
                          )
                    sleep(sleepAmt)
                    retryCount = attempt
                else:
                    raise

        (symbols, count) = self.decodeSymbolsContainer(json)

        for symbol in symbols:
            self.symbols[symbol.ticker] = symbol

        if count > 10:
            # This should never happen with this API, it always returns at most 10 items
            raise Exception("Funny things are happening: count "
                            + text(count)
                            + " > 10. "
                            + "Content:"
                            + "\n"
                            + repr(json))

        if self._getQueryIndex() + 1 >= len(self.queries):
            self.done = True
        else:
            self.done = False

        return symbols

    def decodeSymbolsContainer(self, json):
        symbols = []
        count = 0

        for row in json['data']['items']:
            ticker = text(row['symbol'])
            name = row['name']
            exchange = row['exch']
            exchangeDisplay = row['exchDisp']
            symbolType = row['type']
            symbolTypeDisplay = row['typeDisp']
            symbols.append(Generic(ticker, name, exchange, exchangeDisplay, symbolType, symbolTypeDisplay))

        count = len(json['data']['items'])

        return (symbols, count)

    def getRowHeader(self):
        return SymbolDownloader.getRowHeader(self) + ["exchangeDisplay", "Type", "TypeDisplay"]

