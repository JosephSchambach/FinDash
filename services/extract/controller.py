from extract_rules import rules
from context import Context
from data_extractor import DataExtractor
from data_loader import DataLoader
import pandas as pd

class ExtractController:
    def __init__(self):
        self.context = Context()
        self.extractor = DataExtractor(self.context) 
        self.loader = DataLoader(self.context)
        self.rules = rules(self)
        
    def run(self):
        self.context.logger.info("Starting Extract Process")
        # extract_config = self.context.bigquery.query("select * from extract_config")
        # extract config will be table with the following columns and sample data
        # - id: integer
        # - symbol: string 
        # - source: string (alphavantage or yfinance)
        # - interval: string (1m, 5m, 1h, 1d)
        # - lookback: 
        extract_config = self.context.storage.table('extract_config').select('*').execute()
        extract_config = pd.DataFrame(extract_config.data)
        for index, row in extract_config.iterrows():
            self.context.logger.info(f"Processing {row['symbol']} from {row['source']}")
            rules = self.rules.get(row["source"])
            source = rules.get("method")
            if source: 
                raw_data = source(row["symbol"], row['period'])
                self.extractor.append(raw_data)
            else:
                self.context.logger.warning(f"No extraction method found for {row['source']}")
        self.extractor.save()
        loaded = self.loader.load(self.extractor.file_path, 'raw_extract_data')
        if loaded:
            self.context.logger.info("File loaded successfully")
            self.loader.drop(self.extractor.file_path)
        else:
            self.context.logger.error("File failed to load")

if __name__ == "__main__":
    controller = ExtractController()
    controller.run()