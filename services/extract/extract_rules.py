
def rules(controller):
    return {
        "alphavantage": {
            "method": controller.extractor.alphavantage_extract
        },
        "yfinance": {
            "method": controller.extractor.yfinance_extract
        }
    }