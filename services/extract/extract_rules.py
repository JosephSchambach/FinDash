

def rules(extractor):
    return {
        "alphavantage": {
            "method": extractor.alphavantage_extract
        },
        "yfinance": {
            "method": extractor.yfinance_extract
        }
    }