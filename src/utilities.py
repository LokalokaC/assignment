import logging
import re
import pandas as pd

def transform_string_columns(file_name: str, df:pd.DataFrame, columns: list):
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.upper().str.strip()
        else:
            logging.info(f"{file_name} doesn't have {col}.")
    return df

def transform_timestamp_columns(file_name: str, df: pd.DataFrame, columns: list):
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        else:
            logging.info(f"{file_name} doesn't have {col}.")
    return df

def transform_date_columns(file_name: str, df: pd.DataFrame, date_cols: list, timestamp_cols: list):
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
        else:
            derived = False
            for time_col in timestamp_cols:
                if time_col in df.columns:
                    df[col] = df[time_col].dt.date
                    logging.info(f"{file_name}: '{col}' not found. Use {time_col} to generate it.")
                    derived = True
                    break
            if not derived:
                logging.warning(f"{file_name} is missing both '{col}' and fallback timestamp columns {timestamp_cols}. Cannot derive date.")
    return df

def transform_numeric_columns(file_name: str, df: pd.DataFrame, columns: list):
    if not columns:
        logging.info(f"{file_name} has no numeric columns to transform. Skipping.")
        return df
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            df.loc[df[col] < 0, col] = 0
        else:
            logging.info(f"{file_name} doesn't have {col}.")
    if all(col in df.columns for col in columns):
        rows_before = df.shape[0]
        df = df[~(df[columns].sum(axis=1) == 0)]
        rows_after = df.shape[0]
        dropped = rows_before - rows_after
        if dropped > 0:
            logging.info(f"Dropped {dropped} rows from {file_name} where all {columns} are 0.")
    return df


def check_missing_columns(file_name: str, df:pd.DataFrame, columns: list):  
    for col in columns:
        if col in df.columns:
            missing_col_count = df[col].isnull().sum()
            if missing_col_count > 0:
                missing_col_index = df[df[col].isnull()].index.tolist()
                logging.warning(f"{file_name} has {missing_col_count} rows with invalid or missing {col}!")
                logging.warning(f"{file_name} has missing {col} in {missing_col_index} rows.")
                df = df.dropna(subset=[col])
        else:
            logging.info(f"{file_name} doesn't have {col}.")
    return df

def check_duplications(file_name: str, df:pd.DataFrame, columns: list):
    for col in columns:
        if col in df.columns:
            total_rows = len(df)
            unique_rows = df[columns].drop_duplicates().shape[0]
            duplicate_count = total_rows - unique_rows
            if duplicate_count > 0:
                logging.warning(f"{file_name} has {duplicate_count} duplicated {col} values.")
    return df

def convert_columns_to_snake_case(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [re.sub(r'(?<=[a-z0-9])(?=[A-Z])', '_', col).lower().strip() for col in df.columns]
    return df

class Preprocessor:
    def __init__(
            self,
            file_name: str,
            string_cols: list[str],
            timestamp_cols: list[str],
            date_cols: list[str],
            numeric_cols: list[str],
            duplication_check_cols: list[str],
            missing_check_cols:  list[str]
            ): 
        self.df = None
        self.file_name=file_name
        self.string_cols=string_cols
        self.timestamp_cols=timestamp_cols
        self.date_cols = date_cols or []
        self.numeric_cols=numeric_cols
        self.duplication_check_cols=duplication_check_cols
        self.missing_check_cols=missing_check_cols

    def set_df(self, df: pd.DataFrame):
        self.df = df

    def start(self):
        if self.df is None:
            raise ValueError("DataFrame is not initialized.")
        self.df = transform_string_columns(self.file_name, self.df, self.string_cols)
        self.df = transform_timestamp_columns(self.file_name, self.df, self.timestamp_cols)
        self.df = transform_date_columns(self.file_name, self.df, self.date_cols, self.timestamp_cols)
        self.df = transform_numeric_columns(self.file_name, self.df, self.numeric_cols)
        self.df = check_duplications(self.file_name, self.df, self.duplication_check_cols)
        self.df = check_missing_columns(self.file_name, self.df, self.missing_check_cols)

    def get_df(self) -> pd.DataFrame:
        return self.df
    
    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        self.set_df(df)
        self.start()
        return self.get_df()
