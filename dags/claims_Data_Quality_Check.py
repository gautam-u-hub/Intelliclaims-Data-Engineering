import os
import pandas as pd
import logging

def setup_logging():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(script_dir, 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, 'claimlogs.txt')
    if not os.path.exists(log_file):
        with open(log_file, 'w'):
            pass
    logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def remove_and_save_corrected_csv(df, corrected_dir):
    if not os.path.exists(corrected_dir):
        os.makedirs(corrected_dir)
    corrected_file_path = os.path.join(corrected_dir, 'corrected_claims.csv')
    df.to_csv(corrected_file_path, index=False)
    logging.info("Corrected CSV file saved to: %s", corrected_file_path)

def data_quality_checks():
    file_path = os.path.join(os.path.dirname(__file__), 'files', 'claims.csv')
    df = pd.read_csv(file_path)
    missing_values = df.isnull().sum()
    if missing_values.sum() != 0:
        logging.info("Missing values:\n%s", missing_values)
        df = df.dropna()
    else:
        logging.info("Completeness check passed: No missing values found.")
    duplicate_records = df[df.duplicated()]
    if not duplicate_records.empty:
        logging.info("Duplicate records:\n%s", duplicate_records)
    else:
        logging.info("Uniqueness check passed: No duplicate records found.")
    invalid_claim_amounts = df[df['claimAmount'] <= 0]
    if not invalid_claim_amounts.empty:
        logging.info("Invalid claim amounts:\n%s", invalid_claim_amounts)
        df = df[~df.index.isin(invalid_claim_amounts.index)]
    else:
        logging.info("All claim amounts are valid.")
    valid_statuses = ['Pending', 'Approved', 'Rejected']
    invalid_statuses = df[~df['status'].isin(valid_statuses)]
    if not invalid_statuses.empty:
        logging.info("Invalid status values:\n%s", invalid_statuses)
        df = df[~df.index.isin(invalid_statuses.index)]
    else:
        logging.info("All status values are valid.")
    corrected_dir = os.path.join(os.path.dirname(__file__), 'correctedCSV')
    remove_and_save_corrected_csv(df, corrected_dir)

