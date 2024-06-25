import os
import pandas as pd
from datetime import datetime

def update_and_save_policy_dataset():
    script_directory = os.path.dirname(os.path.abspath(__file__))
    logs_directory = os.path.join(script_directory, 'logs')
    if not os.path.exists(logs_directory):
        os.makedirs(logs_directory)
    log_file = os.path.join(logs_directory, 'update_and_save_policy_dataset.log')
    
    with open(log_file, 'a') as log:
        log.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Starting update and save policy dataset process...\n")

        policy_file = os.path.join(script_directory, 'correctedCSV', 'corrected_policies.csv')
        claims_file = os.path.join(script_directory, 'correctedCSV', 'corrected_claims.csv')
        save_directory = os.path.join(script_directory, 'dataset')
        file_name = 'updated_policies.csv'
        
        try:
            policy_df = pd.read_csv(policy_file)
            claims_df = pd.read_csv(claims_file)
            log.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Policy and claims files loaded successfully.\n")
        except FileNotFoundError as e:
            log.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Error: {e}\n")
            return
        
        closed_statuses = ['Approved', 'Rejected']
        claims_df.loc[claims_df['status'].isin(closed_statuses), 'status'] = 'Closed'

        total_open_claims = {}
        total_closed_claims = {}

        for _, row in claims_df.iterrows():
            policy_id = row['policyId']
            status = row['status']

            total_open_claims[policy_id] = total_open_claims.get(policy_id, 0)
            total_closed_claims[policy_id] = total_closed_claims.get(policy_id, 0)

            if status == 'Closed':
                total_closed_claims[policy_id] += 1
            else:
                total_open_claims[policy_id] += 1

        for index, policy_row in policy_df.iterrows():
            policy_id = policy_row['_id']
            policy_df.at[index, 'total_open_claims'] = total_open_claims.get(policy_id, 0)
            policy_df.at[index, 'total_closed_claims'] = total_closed_claims.get(policy_id, 0)

        policy_df.fillna({'total_open_claims': 0, 'total_closed_claims': 0}, inplace=True)

        policy_df['total_open_claims'] = policy_df['total_open_claims'].astype(int)
        policy_df['total_closed_claims'] = policy_df['total_closed_claims'].astype(int)

        full_directory = os.path.join(save_directory, file_name)
        if not os.path.exists(save_directory):
            os.makedirs(save_directory)
        policy_df.to_csv(full_directory, index=False)
        log.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Updated policy dataset saved to: {full_directory}\n")
        log.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Update and save policy dataset process completed.\n")

