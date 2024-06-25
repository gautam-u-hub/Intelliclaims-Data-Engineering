import os
import pandas as pd
import re
from datetime import datetime

def write_to_log(message, function_name):
    log_file = f"{function_name}_log.txt"
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file, 'a') as log:
        log.write(f"{timestamp} - {message}\n")

def setup_logging():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(script_dir, 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

def remove_and_save_corrected_csv(df, corrected_dir, function_name):
    if not os.path.exists(corrected_dir):
        os.makedirs(corrected_dir)
    corrected_file_path = os.path.join(corrected_dir, f'corrected_users.csv')
    df.to_csv(corrected_file_path, index=False)
    write_to_log(f"Corrected CSV file saved to: {corrected_file_path}", function_name)

def u_data_quality_checks():
    function_name = "u_data_quality_checks"
    setup_logging()

    file_path = os.path.join(os.path.dirname(__file__), 'files', 'users.csv')
    df = pd.read_csv(file_path)

    missing_values = df.isnull().sum()
    if missing_values.sum() != 0:
        write_to_log(f"Missing values:\n{missing_values}", function_name)
        df = df.dropna()
    else:
        write_to_log("Completeness check passed: No missing values found.", function_name)

    duplicate_records = df[df.duplicated()]
    if not duplicate_records.empty:
        write_to_log(f"Duplicate records:\n{duplicate_records}", function_name)
        df = df.drop_duplicates()
    else:
        write_to_log("Uniqueness check passed: No duplicate records found.", function_name)

    valid_email_format = df[df['email'].apply(lambda x: bool(re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', x)))]
    if not valid_email_format.equals(df):
        write_to_log(f"Invalid email formats:\n{df[~df['email'].isin(valid_email_format['email'])]}", function_name)
        df = valid_email_format
    else:
        write_to_log("All email formats are valid.", function_name)

    valid_phone_format = df[df['phoneNumber'].apply(lambda x: bool(re.match(r'^\d{10}$', str(x))))]
    if not valid_phone_format.equals(df):
        write_to_log(f"Invalid phone number formats:\n{df[~df['phoneNumber'].isin(valid_phone_format['phoneNumber'])]}", function_name)
        df = valid_phone_format
    else:
        write_to_log("All phone number formats are valid.", function_name)

    corrected_dir = os.path.join(os.path.dirname(__file__), 'correctedCSV')
    remove_and_save_corrected_csv(df, corrected_dir, function_name)


