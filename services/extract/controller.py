from services.extract.extract_rules import rules
from context import Context
from data_extractor import DataExtractor
from data_loader import DataLoader

class ExtractController:
    def __init__(self):
        self.rules = rules()
        self.context = Context()
        self.extractor = DataExtractor(self.context) 
        self.loader = DataLoader(self.context)
        
    def run(self):
        self.context.logger.info("Starting Extract Process")
        extract_config = self.context.bigquery.query("select * from extract_config")
        # extract config will be table with the following columns and sample data
        # - id: integer
        # - symbol: string 
        # - source: string (alphavantage or yfinance)
        # - interval: string (1m, 5m, 1h, 1d)
        # - 
        for index, row in extract_config.iterrows():
            self.context.logger.info(f"Processing {row['symbol']} from {row['source']}")
            rules = self.rules.get(row["source"])
            source = rules.get("method")
            if source: 
                raw_data = source(row["symbol"])
                self.extractor.append(raw_data)
            else:
                self.context.logger.warning(f"No extraction method found for {row['source']}")
        self.extractor.save()
        self.loader.load(self.extractor.file)
        self.loader.drop(self.extractor.file)