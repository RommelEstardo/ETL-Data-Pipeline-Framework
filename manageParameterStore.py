'''

    Description:

    # This Python script is designed to manage AWS Systems Manager (SSM) Parameter Store parameters. It provides functionality to:
    # 1. Fetch parameters from the SSM Parameter Store based on a specified prefix and save them to a local file.
    # 2. Read parameters from a local file and process them in the SSM Parameter Store, including adding new parameters, editing existing ones, or deleting them.
    # 3. Check if a parameter exists in the SSM Parameter Store.
    # 4. Generate random passwords.
    # The script uses the AWS SDK for Python (Boto3) to interact with the SSM Parameter Store, and it supports operations like fetching, processing (adding, editing, deleting), 
    # and checking parameter existence. It also includes a utility function for generating random passwords.

'''


import boto3
import os
import random
import string

# Replace 'us-west-2' with your AWS region where your SSM Parameter Store is located.
ssm = boto3.client('ssm', region_name='us-west-2')

"""
    Fetches parameters from AWS SSM Parameter Store and saves them to a file.

    This method uses the AWS SDK (boto3) to paginate through the parameters in the SSM Parameter Store that 
    begin with a specified prefix. It then fetches the values of these parameters, decrypts them if necessary, 
    and writes them to a file in the format: "Name = Value (Type: Type)".

    If the directory for the file does not exist, it creates it.

    :param prefix: A string containing the prefix to filter parameters by.
    :param folder_path: A string containing the path to the directory where the file should be saved.
    :param file_name: A string containing the name of the file to save the parameters in.

"""
def fetch_and_save_parameters(prefix, folder_path, file_name):
    paginator = ssm.get_paginator('describe_parameters')
    page_iterator = paginator.paginate (
        ParameterFilters=[{
            'Key': 'Name',
            'Option': 'BeginsWith',
            'Values': [prefix]
        }]
    )

    full_path = os.path.join(folder_path, file_name)
    os.makedirs(os.path.dirname(full_path), exist_ok=True)

    with open(full_path, 'w') as file:
        for page in page_iterator:
            names = [param['Name'] for param in page['Parameters']]
            if names:
                values_response = ssm.get_parameters(Names=names, WithDecryption=True)
                for param in values_response['Parameters']:
                    file.write(f"{param['Name']} = {param['Value']} (Type: {param['Type']})\n")

"""
    Reads parameters from a file and processes them in the AWS SSM Parameter Store.

    This method opens a file and reads it line by line. Each line should contain a parameter name, value, and type 
    in the format: "Name = Value (Type: Type)". It then processes each parameter based on its type. If the type is 
    'DELETE', it deletes the parameter from the SSM Parameter Store. If the type is 'EDIT', it updates the parameter 
    value in the SSM Parameter Store. If the type is neither 'DELETE' nor 'EDIT', it adds the parameter to the SSM 
    Parameter Store.

    If an error occurs while processing a parameter, it logs an error message.

    :param folder_path: A string containing the path to the directory where the file is located.
    :param file_name: A string containing the name of the file to read the parameters from.

"""
def process_parameters_from_file(folder_path, file_name):
    full_path = os.path.join(folder_path, file_name)

    with open(full_path, 'r') as file:
        for line in file:
            # Skip any line that starts with '#'
            if line.strip().startswith('#'):
                continue
            line = line.strip()
            if line:
                name_value, type_part = line.rsplit(' (Type: ', 1)
                name, value = name_value.split('=', 1)
                type_name = type_part.rstrip(')')

                if type_name == 'DELETE':
                    if parameter_exists(name):
                        try:
                            ssm.delete_parameter(Name=name)
                            print(f"Deleted parameter: {name}")
                        except Exception as e:
                            print(f"Error deleting parameter {name}: {str(e)}")
                    else:
                        print(f"Parameter {name} does not exist. No deletion needed.")
                
                elif type_name == 'EDIT':

                    if parameter_exists(name):
                        try:
                            original_parameter = ssm.get_parameter(Name=name, WithDecryption=True)['Parameter']
                            original_type = original_parameter['Type']
                            ssm.put_parameter (
                                Name=name,
                                Value=value.strip(),
                                Type=original_type,
                                Overwrite=True
                            )
                            print(f"Updated value for parameter: {name}, keeping original type: {original_type}")
                        except Exception as e:
                            print(f"Error updating parameter {name}: {str(e)}")
                else:

                    if not parameter_exists(name):
                        try:
                            ssm.put_parameter (
                                Name=name,
                                Value=value,
                                Type=type_name,
                                Overwrite=False
                            )
                            print(f"Added new parameter: {name} with type: {type_name}")
                        except Exception as e:
                            print(f"Failed to add {name}: {str(e)}")
                    else:
                        print(f"Parameter {name} already exists. No action performed.")

"""
    Checks if a parameter exists in the AWS SSM Parameter Store.

    This method uses the AWS SDK (boto3) to attempt to get a parameter from the SSM Parameter Store with the 
    specified name. If the parameter exists, it returns True. If the parameter does not exist, it catches the 
    ParameterNotFound exception and returns False.

    If an error occurs while checking the parameter, it logs an error message and returns False.

    :param name: A string containing the name of the parameter to check.
    :return: A boolean indicating whether the parameter exists.
    
"""

def parameter_exists(name):

    try:
        ssm.get_parameter(Name=name)
        return True
    except ssm.exceptions.ParameterNotFound:
        return False
    except Exception as e:
        print(f"Error checking parameter existence for {name}: {str(e)}")
        return False
    
"""
    Manages AWS SSM Parameter Store parameters based on the specified operation.

    This method acts as a controller that calls either `fetch_and_save_parameters` or `process_parameters_from_file` 
    based on the operation argument. If the operation is 'fetch', it fetches parameters from the SSM Parameter Store 
    and saves them to a file. If the operation is 'process', it reads parameters from a file and processes them in 
    the SSM Parameter Store.

    If the operation is not 'fetch' or 'process', it prints an error message.

    :param operation: A string containing the operation to perform. Valid values are 'fetch' and 'process'.
    :param prefix: A string containing the prefix to filter parameters by. Only used if operation is 'fetch'.
    :param folder_path: A string containing the path to the directory where the file is located or should be saved.
    :param file_name: A string containing the name of the file to read the parameters from or save the parameters in.

"""

def manage_parameters(operation, prefix=None, folder_path=None, file_name=None):
    if operation == 'fetch':
        fetch_and_save_parameters(prefix, folder_path, file_name)
    elif operation == 'process':
        process_parameters_from_file(folder_path, file_name)
    else:
        print("Invalid operation. Use 'fetch' to retrieve parameters or 'process' to update or delte them.")


"""
    Generates a random password of a specified length.

    This method creates a string of all possible characters (letters, digits, and punctuation), and then selects 
    a random character from this string for each character in the password. It returns the resulting password.

    :param length: An integer specifying the length of the password to generate. Defaults to 12.
    :return: A string containing the generated password.

"""

def generate_password(length=12):
    characters = string.ascii_letters + string.digits + string.punctuation
    password = ''.join(random.choice(characters) for i in range(length))
    return password


# enable to generate random passwords if needed
#for j in range(10):
#    print(generate_password(20))

'''
    # Uncomment the manage_parameters function call below and run it initially to fetch parameters from AWS SSM and save them to a local file.
    # Customize 'prefix', 'folder_path', and 'file_name' as needed.
'''
# manage_parameters('fetch', prefix='/', folder_path='e:\ETLsolutions\parameterStore', file_name='parameters.txt')

# Process parameters from a local file and update AWS SSM Parameter Store. Customize 'folder_path' and 'file_name' as needed.
manage_parameters('process', folder_path='e:\ETLsolutions\parameterStore', file_name='parameters.txt')

# Fetch updated parameters from AWS SSM and save them again to a local file for verification. Customize 'prefix', 'folder_path', and 'file_name' as needed.
manage_parameters('fetch', prefix='/', folder_path='e:\ETLsolutions\parameterStore', file_name='parameters.txt')