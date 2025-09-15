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
        # Doing the below in bigquery would look like this: 
        # query_job = self.context.bigquery.query("select * from extract_config")
        # extract_config = query_job.to_dataframe()
        extract_config = self.context.storage.table('extract_config').select('*').execute()
        extract_config = pd.DataFrame(extract_config.data)
        for index, row in extract_config.iterrows():
            self.context.logger.info(f"Processing {row['symbol']} from {row['source']}")
            rules = self.rules.get(row["source"])
            try:
                method = rules.get("method")
            except Exception as e:
                self.context.logger.error(f"Error getting method for {row['source']}: {e}")
                continue
            if method: 
                raw_data = method(row["symbol"], row['period'])
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
    controller.context.logger.info("Extract Process Completed")