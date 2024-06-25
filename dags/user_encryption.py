import os
import pandas as pd
from cryptography.fernet import Fernet
import secrets
from datetime import datetime

def write_to_log(message, log_file):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file, 'a') as log:
        log.write(f"{timestamp} - {message}\n")

def generate_secret_key():
    return Fernet.generate_key()

def encrypt_id(id, key):
    cipher_suite = Fernet(key)
    return cipher_suite.encrypt(id.encode()).decode()

def mask_mobile_number(mobile_number):
    mobile_str = str(mobile_number)
    
    if len(mobile_str) >= 10:
        return '*' * 6 + mobile_str[-4:]
    else:
        return mobile_str
    

def update_and_save_user_dataset():
    function_name = "update_and_save_user_dataset"
    script_directory = os.path.dirname(os.path.abspath(__file__))
    logs_directory = os.path.join(script_directory, 'logs')
    if not os.path.exists(logs_directory):
        os.makedirs(logs_directory)

    log_file = os.path.join(logs_directory, f"{function_name}_log.txt")

    encrypted_directory = os.path.join(script_directory, 'encrypted')
    if not os.path.exists(encrypted_directory):
        os.makedirs(encrypted_directory)

    users_file = os.path.join(script_directory, 'correctedCSV', 'corrected_users.csv')
    encrypted_users_file = os.path.join(encrypted_directory, 'encrypted_users.csv')

    try:
        users_df = pd.read_csv(users_file)
    except FileNotFoundError as e:
        error_message = f"Error: {e}"
        print(error_message)
        write_to_log(error_message, log_file)
        return

    key = generate_secret_key()

    users_df['_id'] = users_df['_id'].apply(lambda x: encrypt_id(x, key))
    users_df['phoneNumber'] = users_df['phoneNumber'].apply(mask_mobile_number)

    users_df.to_csv(encrypted_users_file, index=False)
    success_message = f"Updated user dataset saved to: {encrypted_users_file}"
    print(success_message)
    write_to_log(success_message, log_file)

