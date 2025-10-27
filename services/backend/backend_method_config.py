from stock_data import StockDataService

def get_backend_config(context):
    return {
        "stock": {
            "method": StockDataService(context).fetch_stock_data,
        }
    }