from datetime import datetime

import pandas as pd


def process_and_store_data(companies):
    df = pd.DataFrame(companies)
    df['extraction_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_name = f'connectwise_companies_{timestamp}.parquet'
    df.to_parquet(file_name, index=False)
    return file_name, df
