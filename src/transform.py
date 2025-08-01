import logging
import pandas as pd
import json
from pathlib import Path
from src.utilities import convert_columns_to_snake_case, Preprocessor

SRC_DIR = Path(__file__).resolve().parent
TMP_DIR = Path(__file__).resolve().parent.parent / "tmp"
CONFIG_PATH = SRC_DIR/'column_config.json'

def transform_and_merge(input_paths: list[str]) -> str:
    """
    Clean and merge the DataFrames based on specified data quality requirements:
        Handle missing, non-numeric, or non-positive Quantity/UnitPrice values
        Treat invalid entries as 0 for calculations
        Perform case-insensitive region normalization
    Args:
        intput_paths (dict): {file_name: output_path} mapping, retrieved from extract_data()
    Returns:
        dict[str, str]: A cleaned and merged .parquet ready for insertion into the Orders table.
    """
    
    with open(CONFIG_PATH, "r") as f:
        COLUMN_CONFIGS = json.load(f)
    
    processed_dfs = {} 

    for input_path_str in input_paths:
        input_path = Path(input_path_str)
        file_name = input_path.stem

        df = pd.read_parquet(input_path)
        if df is None:
            logging.warning(f"No DataFrame found for {file_name}, skipping.")
            continue
        conf=COLUMN_CONFIGS[file_name]

        processor = Preprocessor(
            file_name=file_name,
            string_cols=conf.get("string_cols",[]),
            timestamp_cols=conf.get("timestamp_cols"),
            date_cols=conf.get("date_cols"),
            numeric_cols=conf.get("numeric_cols"),
            duplication_check_cols=conf.get("duplication_check_cols"),
            missing_check_cols=conf.get("missing_check_cols")
        )
        processed_dfs[file_name] = processor.run(df)
    if len(processed_dfs) != 2:
        raise ValueError(f"Expected 2 DataFrames but got {len(processed_dfs)}")
    
    file_name_1, file_name_2 = processed_dfs.keys()
    df1, df2 = processed_dfs.values()
    missing_dates = df2[df2['TransactionDate'].isnull()]
    if len(missing_dates) > 0:
        logging.warning(f"{file_name_2} has {len(missing_dates)} missing rows of TransactionDate. Will parse from Timestamp.")
        df2['TransactionDate'] = df2['TransactionDate'].fillna(df2['Timestamp'].dt.date)

    merged_df = pd.merge(
        df1, 
        df2, 
        on=['OrderID','TransactionDate'], 
        how='inner', 
        suffixes=('_1', '_2')
        )
    if merged_df['OrderID'].isnull().any():
        logging.warning("Some OrderIDs are missing after merge. These rows require further handling or might be dropped.")

    merged_df = merged_df.rename(columns={'Timestamp_1': 'Timestamp'})

    columns_to_keep = ['OrderID','Timestamp','Region','CustomerID'
                       ,'ProductCategory','ProductName','Quantity','UnitPrice','ShippingCost','TotalAmount'
                       ,'TaxAmount','PaymentMethod']
    df = merged_df[columns_to_keep]

    df = convert_columns_to_snake_case(df)

    logging.info(f"Order data has been transformed and merged successfully. Dataset shape: {df.shape}")
    
    output_path = TMP_DIR / "merged_order.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path)
    
    logging.info(f"Saved merged DataFrame to {output_path}")
    return str(output_path)

def transform(input_paths: list[str]) -> str:
    """
    Transform the data types of the columns.
    Args:
        intput_paths (dict): {file_name: output_path} mapping, retrieved from extract_data()
    Returns:
        dict[str, str]: A cleaned and merged .parquet ready for insertion into the customer_activities table.
    """
    logging.info("Starting processing customer.log.")

    with open(CONFIG_PATH, "r") as f:
        COLUMN_CONFIGS = json.load(f)
    if not input_paths:
        raise ValueError("No input paths provided to transform().")    
    for input_path_str in input_paths:
        input_path = Path(input_path_str)
        file_name = input_path.stem

        df = pd.read_parquet(input_path)
        if df is None:
            logging.warning(f"No DataFrame found for {file_name}, skipping.")
            continue
        conf=COLUMN_CONFIGS[file_name]

        processor = Preprocessor(
            file_name=file_name,
            string_cols=conf.get("string_cols",[]),
            timestamp_cols=conf.get("timestamp_cols"),
            date_cols=conf.get("date_cols"),
            numeric_cols=conf.get("numeric_cols"),
            duplication_check_cols=conf.get("duplication_check_cols"),
            missing_check_cols=conf.get("missing_check_cols")
        )
        cleaned_df = processor.run(df)
    cleaned_df = convert_columns_to_snake_case(cleaned_df)

    output_path = TMP_DIR / f"cleaned_{file_name}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cleaned_df.to_parquet(output_path)

    logging.info(f"Saved cleaned {len(cleaned_df)} rows of DataFrame to {output_path}")
    return str(output_path)