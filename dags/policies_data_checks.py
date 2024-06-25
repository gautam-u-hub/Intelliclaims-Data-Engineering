import os
import pandas as pd
from datetime import datetime

def write_to_log(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_file = os.path.join(os.path.dirname(__file__), 'logs', 'policy_data_quality_logs.txt')
    with open(log_file, 'a') as log:
        log.write(f"{timestamp} - {message}\n")

def remove_and_save_corrected_csv(df, corrected_dir):
    if not os.path.exists(corrected_dir):
        os.makedirs(corrected_dir)
    corrected_file_path = os.path.join(corrected_dir, 'corrected_policies.csv')
    df.to_csv(corrected_file_path, index=False)
    write_to_log(f"Corrected CSV file saved to: {corrected_file_path}")

def p_data_quality_checks():
    setup_logging()
    file_path = os.path.join(os.path.dirname(__file__), 'files', 'policies.csv')
    df = pd.read_csv(file_path)
    
    write_to_log("Starting data quality checks...")

    missing_values = df.isnull().sum()
    if missing_values.sum() != 0:
        write_to_log(f"Missing values:\n{missing_values}")
        df = df.dropna()
    else:
        write_to_log("Completeness check passed: No missing values found.")

    min_premium = 10
    max_premium = 100000000000
    invalid_premium_range = df[(df['premiumAmount'] < min_premium) | (df['premiumAmount'] > max_premium)]
    if not invalid_premium_range.empty:
        write_to_log(f"Invalid premium amounts:\n{invalid_premium_range}")
        df = df[~df.index.isin(invalid_premium_range.index)]
    else:
        write_to_log("All premium amounts are within the valid range.")

    min_policy_term = 1
    max_policy_term = 100
    invalid_policy_terms = df[(df['policyTerm'] < min_policy_term) | (df['policyTerm'] > max_policy_term)]
    if not invalid_policy_terms.empty:
        write_to_log(f"Invalid policy terms:\n{invalid_policy_terms}")
        df = df[~df.index.isin(invalid_policy_terms.index)]
    else:
        write_to_log("All policy terms are within the valid range.")

    corrected_dir = os.path.join(os.path.dirname(__file__), 'correctedCSV')
    remove_and_save_corrected_csv(df, corrected_dir)

def setup_logging():
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, 'policy_data_quality_logs.txt')
    if not os.path.exists(log_file):
        with open(log_file, 'w'):
            pass


