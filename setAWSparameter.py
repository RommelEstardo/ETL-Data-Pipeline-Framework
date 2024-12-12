'''
    Description:
    
    # This script uses the AWS Boto3 library to interact with AWS Systems Manager (SSM) Parameter Store.
    # It sets three SecureString parameters in the SSM Parameter Store with redacted values for security reasons:
    # 1. 'sftp_password'
    # 2. 'smtp_password'
    # 3. 'sql_password'
    # Each parameter is created or updated with the specified name and marked as a SecureString, with overwrite enabled.

'''

import boto3

# Replace region_name with the AWS Region in which you want to create the parameters.
ssm = boto3.client('ssm', region_name='us-west-2') 

# SFTP Password
ssm.put_parameter(
    Name='sftp_password',  
    Value='', # Replace the empty string in 'Value' with your actual password for the parameter being set.
    Type='SecureString',
    Overwrite=True
)

# Mail Server Password
ssm.put_parameter(
    Name='smtp_password',  
    Value='', # Replace the empty string in 'Value' with your actual password for the parameter being set. 
    Type='SecureString',
    Overwrite=True
)

# SQL Server Password
ssm.put_parameter(
    Name='sql_password',  
    Value='', # Replace the empty string in 'Value' with your actual password for the parameter being set.  
    Type='SecureString',
    Overwrite=True
)