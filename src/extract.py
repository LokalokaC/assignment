import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "tmp"

def extract_data(file_names: list[str]) -> list[str]:
    """
    Read .csv or .log files, convert them to DataFrames, and save as .parquet in /tmp.
    Args:
        file_names (list): order_data_1.csv / order_data_2.csv / customer.log
    Returns:
        dict[str, str]: Mapping of original filename to .parquet file path.
    """
    output_paths = []

    for file_name in file_names:

        file_path = DATA_DIR / file_name
        
        if not file_path.is_file():
            raise FileNotFoundError(f"Error: {file_name} not found in {file_path}.")
        
        try:
            if file_path.suffix == '.csv':
                df = pd.read_csv(file_path, chunksize=None)

            elif file_path.suffix == '.log':
                columns = ['Timestamp', 'CustomerID', 'Activity']
                df = pd.read_csv(file_path, sep=' ', header=None, names=columns, chunksize=None)

                for col in df.select_dtypes(include=['object']).columns:
                    df[col] = df[col].astype(str).str.replace('[', '', regex=False).str.replace(']', '', regex=False).str.strip()
            
            else:
                logging.error(f"Unsupported file type: {file_path.suffix}")
                raise ValueError(f"Unsupported file type: {file_path.suffix}")
            
            logging.info(f"Successfully extracted {len(df)} rows from {file_name}.")

            parquet_path = OUTPUT_DIR / f"{Path(file_name).stem}.parquet"
            parquet_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(parquet_path, index=False)
            output_paths.append(str(parquet_path))

            logging.info(f"Saved {file_name} to {parquet_path}")
            
        except Exception as e:
            logging.exception(f"Error reading CSV/LOG file {file_path.name}: {e}")
            raise    
    return output_paths