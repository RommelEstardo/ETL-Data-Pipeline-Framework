#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""


Description:
    ------------
    This ETL (Extract, Transform, Load) module is designed to facilitate the automated processing of data from 
    various sources and import it into a SQL Server database. The module provides a flexible and secure approach 
    to handling sensitive information, integrating with AWS Systems Manager (SSM) for secure password storage 
    and retrieval.

    The module consists of the following main classes:
    
    1. EmailUtility:
       --------------
       - Purpose: To send email notifications with custom subjects and body content.
       - Features: 
           - Integration with AWS SSM for secure retrieval of SMTP credentials.
           - Supports TLS encryption and customizable SMTP server configurations.

    2. ETLProcess:
       -----------
       - Purpose: To orchestrate the entire ETL process, from downloading files to processing and importing data.
       - Features:
           - Handles data from multiple sources: 
               - URL: Downloads files from specified URLs.
               - S3: Downloads files from AWS S3 buckets.
               - SFTP: Downloads files from secure FTP servers.
               - Local: Processes files stored in local directories.
           - Supports CSV, JSON, and ZIP file formats.
           - Offers multiple import methods: 
               - BCP (Bulk Copy Program) for large data imports into SQL Server.
               - Bulk Insert for efficient data transfer.
               - Pandas for flexible data manipulation and insertion.
           - Includes logging for detailed tracking of the ETL process.
           - Provides utilities for file extraction, handling compressed files, and archiving processed data.
           - Configurable through INI files for dynamic ETL process management.


"""

import configparser
import logging
import requests
import os
import glob
import zipfile
import subprocess
import pyodbc
import shutil
import csv
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import pandas as pd
import boto3
import paramiko
import json
from sqlalchemy import create_engine
import numpy as np

# EmailUtility class
class EmailUtility:

    
    def __init__(self, email_config):
        
        """


        Initializes the EmailUtility instance.

        This constructor method sets up the initial configuration for sending emails.
        It uses the provided `email_config` dictionary to configure the SMTP server,
        port, user, and recipient information, and securely retrieves the SMTP password
        from AWS Systems Manager (SSM) Parameter Store.


        """


        # Initialize the AWS SSM client to securely retrieve the SMTP password
        ssm = boto3.client('ssm', region_name='us-west-2')  

        # Retrieves and stores the SMTP password from AWS SSM using the key 'smtp_password
        self.smtp_password = ssm.get_parameter(Name='smtp_password', WithDecryption=True)['Parameter']['Value']

        # Configures the SMTP server, port, user, and recipient using the values from the 'email_config' dictionary
        self.server = email_config['smtp_server']
        self.port = email_config['smtp_port'] 
        self.user = email_config.get('user')
        self.recipient = email_config['recipient']


    def send_email(self, subject, body):

        # Create a multipart MIME message with sender, recipient, and subject headers
        msg = MIMEMultipart()
        msg['From'] = self.user if self.user else 'anonymous@example.com'
        msg['To'] = self.recipient
        msg['Subject'] = subject

        # Attach the body of the email, with the content type set to 'plain' text
        msg.attach(MIMEText(body, 'plain'))

        # Establish a connection to the SMTP server and start TLS encryption
        server = smtplib.SMTP(self.server, self.port)
        server.starttls()

        # If a user and SMTP password are provided, use them to log in to the SMTP server
        if self.user and self.smtp_password:
            server.login(self.user, self.smtp_password)

        # Send the email, close the SMTP server connection, and log the successful email sending
        server.sendmail(self.user if self.user else 'anonymous@example.com', self.recipient, msg.as_string())
        server.quit()
        logging.info("Email sent successfully")

# ETLProcess class
class ETLProcess:

    """
        Initializes the ETLProcess instance.

        This method sets up the ETL process configuration by reading from a provided configuration file.
        It retrieves the SQL password from AWS Systems Manager (SSM) Parameter Store.
        It also sets up logging and initializes an EmailUtility instance for email notifications.

        The configuration includes details about the database (type, server, name, user, table name, etc.), 
        the ETL process (field delimiter, file name, file prefix, file suffix, file extensions, file type, 
        whether the file has a header, archive path, etc.), and the import method (whether to use BCP import, 
        bulk insert, or pandas import).

        :param config_file: A string containing the path to the configuration file.
    
    """

    def __init__(self, config_file):

        # Initialize the AWS SSM client
        ssm = boto3.client('ssm', region_name='us-west-2')  

        # Retrieve the SQL password from AWS SSM
        self.pwd = ssm.get_parameter(Name='sql_password', WithDecryption=True)['Parameter']['Value']

        # Initialize the configuration parser and read the provided configuration file
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

        # Set up logging
        self.setup_logging()

        # Initialize the email utility and database configuration from the provided configuration
        self.email_util = EmailUtility(self.config['EMAIL'])
        self.db_type = self.config['ETL']['database_type']      
        self.dbServer = self.config['MSSQL']['server']
        self.dbName = self.config['MSSQL']['database']            
        self.uid = self.config['MSSQL']['user']
        self.tableName = self.config['MSSQL']['table_name']
        self.drop_table_if_exists = self.config['MSSQL'].getboolean('drop_table_if_exists')

        # Get the field delimiter from the configuration
        delimiter = self.config['ETL']['field_delimiter']     

        # If the delimiter is a tab, enclose it in quotes
        if delimiter == r'\t':
            self.field_delimiter = '\t'                  # this becomes a string
        else:
            self.field_delimiter = delimiter

        # Extract file-related configuration details from the ETL configuration
        self.file_name = self.config['ETL']['file_name']
        self.file_prefix = self.config['ETL']['file_prefix']
        self.file_suffix = self.config['ETL']['file_suffix']
        self.file_extensions = self.config['ETL']['file_extensions']
        self.file_type = self.config['ETL']['file_type']
        self.file_has_header = self.config['ETL'].getboolean('file_has_header')
        self.archive_path = self.config['ETL']['archive_path']

        # Set the end of row character for BCP, handling the special case where the end of row character is a newline
        bcp_end_of_row = self.config['ETL']['bcp_end_of_row']
        if bcp_end_of_row == r'\n':
            self.bcp_end_of_row = '"\\n"'   
        else:
            self.bcp_end_of_row = bcp_end_of_row

        # Extract BCP-related configuration details from the ETL configuration
        self.bcp_row_start = self.config['ETL']['bcp_row_start']
        self.bcp_batch_commit_size = self.config['ETL']['bcp_batch_commit_size']
        self.bcp_end_of_row = self.config['ETL']['bcp_end_of_row']

        # Extract import method preferences from the ETL configuration
        self.bcp_import_bool = self.config['IMPORT_METHOD'].getboolean('bcp_import')
        self.bulkInsert_import_bool = self.config['IMPORT_METHOD'].getboolean('bulkInsert_import')
        self.pandas_import_bool = self.config['IMPORT_METHOD'].getboolean('pandas_import')
   

    """
        Empties a folder of all ZIP and CSV files.

        This method searches for all ZIP and CSV files in the specified folder and removes them.
        It also removes any other files and directories in the folder.

        :param folder_path: A string containing the path to the folder to be emptied.

    """
    def empty_folder_of_zip_csv(self, folder_path):
        
        # Define the file patterns for ZIP and CSV files
        file_patterns = ['*.zip', '*.csv']

        # Iterate over the defined file patterns
        for pattern in file_patterns:
            # Construct the full pattern by joining the folder path and the file pattern
            full_pattern = os.path.join(folder_path, pattern)

            # Iterate over all files matching the full pattern
            for filename in glob.glob(full_pattern):
                try:
                    # Attempt to remove the file and print a message if successful
                    os.remove(filename)
                    print(f"Removed {filename}")
                except OSError as e:
                    # Print an error message if the file could not be removed
                    print(f"Error: {e.strerror}")

        # Iterate over all files and directories in the specified folder
        for filename in os.listdir(folder_path):
            # Construct the full file path by joining the folder path and the filename
            file_path = os.path.join(folder_path, filename)
            try:
                # If the file path is a file or a symbolic link, attempt to unlink (remove) it
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                # If the file path is a directory, attempt to remove it and all its contents
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                # Print an error message if the file or directory could not be removed
                print(f'Failed to delete {file_path}. Reason: {e}')                    


    """
        Sets up logging for the ETL process.

        This method configures the logging module to log INFO and higher level messages.
        Logs are written to a file named 'etl_log.log' in the current directory.
        Each log message includes the timestamp, the severity level, and the actual message.

    """
    def setup_logging(self):
        # Configure the logging module to log INFO and higher level messages to a file named 'etl_log.log'
        logging.basicConfig(filename='etl_log.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    """
        Downloads a file from a given URL and saves it to a specified location.

        This method sends a GET request to the provided URL and writes the response content to a file.
        If the HTTP request returns an unsuccessful status code, it raises an HTTPError.
        If the URL is invalid, it logs an error message.

        :param url: A string containing the URL of the file to be downloaded.
        :param targetFile: A string containing the path where the downloaded file should be saved.
    
    """

    def download_from_url(self, url, targetFile):

        # Attempt to download a file from the given URL and save it to the target file
        try:
            # Send a GET request to the URL
            response = requests.get(url)
            # Raise an exception if the response contains an HTTP error status code
            response.raise_for_status()  

            # Open the target file in write-binary mode
            with open(targetFile, 'wb') as f:
                # Write the content of the response to the file
                f.write(response.content)

            # Log a message indicating that the file was downloaded successfully
            logging.info(f"File downloaded: {targetFile}")

        # Handle the specific exception that is raised when the URL is invalid
        except requests.exceptions.MissingSchema:
            # Log an error message indicating that the URL is invalid
            logging.error(f"Invalid URL: {url}")

    """
        Downloads files from a specified S3 bucket and folder to a local destination folder.

        This method lists all objects in the specified S3 bucket and folder, and downloads the ones 
        that match the file prefix and extensions specified in the ETL configuration. 
        It uses the boto3 library to interact with the S3 service.

        :param s3_bucket: A string containing the name of the S3 bucket.
        :param s3_folder: A string containing the name of the folder in the S3 bucket.
        :param destination_folder: A string containing the path to the local folder where the files should be downloaded.
    
    """

    def download_from_s3(self, s3_bucket, s3_folder, destination_folder):

        # Initialize the S3 client
        s3 = boto3.client('s3')

        # List objects in the specified S3 bucket
        objects = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_folder)
        files = [item['Key'] for item in objects['Contents'] if not item['Key'].endswith('/')]

        # Initialize a list to hold the file name and extensions
        files_without_folders = []
        file_extensions = []        

        # Iterate over the files, extract the file names without folder prefixes and their extensions
        for file in files:
            if not file.endswith('/'):
                # Split the key by '/' and take the last part to get the file name without folder prefix
                file_name = file.split('/')[-1]
                files_without_folders.append(file_name) 

                # Use os.path.splitext to get the file extension and add it to the list
                _, file_extension = os.path.splitext(file_name)
                file_extensions.append(file_extension)                

        # Filter files based on prefix and extension, then download matching files from S3 to the local destination
        for file_name in files_without_folders:

            # Check if the object key starts with the desired prefix
            if file_name.startswith(self.file_prefix):
                # Check if the file extension matches the desired extensions
                _, file_extension = os.path.splitext(file_name)  
                
                # If the file extension matches the desired extensions, construct the destination path for the file
                file_extension = file_extension[1:]  # Remove the prefixed dot             
                if file_extension in self.file_extensions:
                    destination_path = os.path.join(destination_folder, file_name)

                    # Copy the file from S3 to the local destination
                    s3.download_file(s3_bucket, s3_folder+file_name, destination_path)
                    print(f"Copied s3://{s3_bucket}{s3_folder}{file_name} to {destination_path}")


    """
        Downloads files from a specified SFTP server and path to a local destination path.

        This method connects to the SFTP server using the provided host, port, username, and password.
        It lists all files in the specified remote path and downloads the ones that match the file prefix 
        and extensions specified in the ETL configuration. If a downloaded file is a ZIP file, it extracts 
        the contents to the local path.

        :param host: A string containing the hostname of the SFTP server.
        :param port: An integer representing the port number of the SFTP server.
        :param username: A string containing the username for the SFTP server.
        :param password: A string containing the password for the SFTP server.
        :param remote_path: A string containing the path on the SFTP server from where the files should be downloaded.
        :param local_path: A string containing the path to the local folder where the files should be downloaded.
    
    """
    def download_from_sftp(self, host, port, username, password, remote_path, local_path):
        
        # Create an SSH client instance
        ssh = paramiko.SSHClient()
        # Set the policy to automatically add the server's host key (prevents needing to manually add it)
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            # Connect to the SFTP server using the provided host, port, username, and password
            ssh.connect(host, port, username, password)

            # Create an SFTP client from the SSH client
            sftp = ssh.open_sftp()

            # List all files in the remote directory
            files = sftp.listdir(remote_path)

            # Split the extensions string into a list
            extensionList = [ext.strip() for ext in self.file_extensions.split(',')]

            # Filter files based on prefix and any of the extensions
            filtered_files = [f for f in files if f.startswith(self.file_prefix) or any(f.endswith(self.file_suffix + '.' + ext) for ext in extensionList)]

            # Download each filtered file
            for file_name in filtered_files:
                remote_file_path = os.path.join(remote_path, file_name)
                local_file_path = os.path.join(local_path, file_name)
                sftp.get(remote_file_path, local_file_path)
                print(f"Downloaded {file_name} to {local_path}")

                #Check if the file is a ZIP file and unzip it
                if zipfile.is_zipfile(local_file_path):
                   with zipfile.ZipFile(local_file_path, 'r') as zip_ref:
                       # Define an extraction path (can customize or use same directory)
                       extract_path = os.path.join(local_path, os.path.splitext(file_name)[0])
                       zip_ref.extractall(extract_path)
                       print(f"Extracted {file_name} to {extract_path}")

        except Exception as e:
            print(f"Failed to download files: {e}")
        finally:
            # Close the SFTP session and client
            sftp.close()
            ssh.close()

    """
        Extracts the contents of a file if it is a ZIP file.

        This method checks if the file at the provided path is a ZIP file. If it is, it extracts the contents 
        of the ZIP file to the same directory. It then checks if a file with the same name already exists in 
        the archive directory. If it does, it deletes the existing file. Finally, it moves the ZIP file to the 
        archive directory.

        :param file_path: A string containing the path to the file to be extracted.

    """
    def extract_file_if_compressed(self, file_path):

        # Check if the file is a zip file
        if os.path.splitext(file_path)[1] == '.zip':
            # If it is, open the zip file in read mode
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                # Extract all files in the zip file to the same directory as the zip file
                zip_ref.extractall(os.path.dirname(file_path))
            # Log a message indicating that the zip file has been extracted
            logging.info(f"Extracted {file_path}")

            # Construct the path where the zip file will be moved to after extraction
            dest_file_path = os.path.join(self.archive_path, os.path.basename(file_path))

            # If a file already exists at the destination path, remove it
            if os.path.exists(dest_file_path):
                os.remove(dest_file_path)

            # Move the zip file to the archive folder
            print(f"Moving {file_path} to the archive folder {self.archive_path} ...")
            shutil.move(file_path, self.archive_path)
 

    """
        Extracts the contents of a file if it is a ZIP file.

        This method checks if the file at the provided path is a ZIP file. If it is, it extracts the contents 
        of the ZIP file to the same directory. It then checks if a file with the same name already exists in 
        the archive directory. If it does, it deletes the existing file. Finally, it moves the ZIP file to the 
        archive directory.

        :param file_path: A string containing the path to the file to be extracted.

    """
    def bcp_import(self, file_path, tableName):

        # Assign the field delimiter from the ETL configuration to a local variable     
        delimiter = self.field_delimiter

        # If the delimiter is a tab, replace it with a string representation of a tab for compatibility
        if delimiter == '\t':
            delimiter = '"\\t"' 

        # Construct the BCP command string with the appropriate parameters for database, table, file path, batch size, delimiter, server, and end of row character
        bcp_command = f"bcp {self.dbName}.dbo.{tableName}_View IN {file_path} -F {self.bcp_row_start} -c \
            -b {self.bcp_batch_commit_size} -t{delimiter} -S {self.dbServer} -r {self.bcp_end_of_row} "
        
        # If a user ID is provided, add it and the password to the BCP command; otherwise, add the trusted connection flag
        if self.uid>'':
            bcp_command += f"-U {self.uid} -P {self.pwd}"
        else:        
            bcp_command += "-T" 

        # Execute the BCP command using a subprocess, handling and logging any exceptions that occur
        try:
            subprocess.run(bcp_command, shell=True)
        except Exception as e:
            print(f'BCP import failed: {e}')
        else:
            print('BCP import succeeded')


    """
        Imports data into a SQL Server database using the BCP utility.

        This method constructs a BCP command to import data from a file into a SQL Server database table.
        The BCP command includes the database name, table name, file path, field delimiter, row start, 
        batch commit size, end of row character, and database server. If a user ID and password are provided, 
        they are included in the command; otherwise, the command uses trusted connection (-T option).

        If the BCP command fails, it logs an error message. If it succeeds, it logs a success message.

        :param file_path: A string containing the path to the file to be imported.
        :param tableName: A string containing the name of the database table where the data should be imported.

    """
    def bulkInsert_import(self, file_path, tableName):

        # Establish a connection to the database and create a cursor for executing SQL commands
        conn = self.connect_to_database()
        cursor = conn.cursor()    

        # Construct a SQL command to bulk insert data from the file into the specified table, with the appropriate field and row terminators, starting row, and batch size
        sql = f"""
        BULK INSERT {tableName}
        FROM '{file_path}'
        WITH (
            FIELDTERMINATOR = '{self.field_delimiter}',
            ROWTERMINATOR = '{self.bcp_end_of_row}',
            FIRSTROW = {self.bcp_row_start},
            BATCHSIZE = {self.bcp_batch_commit_size}
        )
        """

        # Execute the SQL command to perform a bulk insert, commit the transaction, handle any errors, and close the cursor and connection
        try:
            cursor.execute(sql)
            conn.commit()
        except pyodbc.Error as e:
            print(f'BULK INSERT failed: {e}')
        else:
            print('BULK INSERT succeeded')
        finally:
            cursor.close()
            conn.close()        

    """
        Imports data into a SQL Server database using pandas.

        This method reads data from a file into a pandas DataFrame, converts the data to the appropriate types, 
        and then inserts the data into a SQL Server database table. It uses the existing database connection method 
        to connect to the database.

        The method handles various data formats, such as values enclosed in parentheses (which are converted to negative), 
        "-" (which is converted to None), and "<NA>" (which is also converted to None).

        If the import fails, it logs an error message. If it succeeds, it logs a success message and prints the number 
        of rows imported.

        :param file_path: A string containing the path to the file to be imported.
        :param tableName: A string containing the name of the database table where the data should be imported.
    
    """
    def pandas_import(self, file_path, tableName):
        
        try:
            # Replace any double quotes in the field delimiter and use it to read the file into a pandas DataFrame
            delimiter = self.field_delimiter.replace('"', '')
            df = pd.read_csv(file_path, delimiter=delimiter, engine='python')

            # Define a function to convert string values to appropriate data types, handling optional double quotes, negative values enclosed in parentheses, and missing values represented as "-" or "<NA>"
            def convert_values(val):
                if isinstance(val, str):
                    val = val.strip('"')            # Remove optional double quotes
                    if val.startswith("(") and val.endswith(")"):
                        return -float(val[1:-1])    # Convert values enclosed in parentheses to negative
                    elif val == "-":
                        return None                 # Convert "-" to None
                    elif val == "<NA>":
                        return None                 # Convert "<NA>" to None
                    # add other conditions as needed
                return val                          # Return the original value
            
            # Apply the function to each column in the DataFrame
            df = df.apply(lambda col: col.apply(convert_values)).convert_dtypes()

            # Convert the DataFrame's data types to string
            df = df.astype(str)

            # Replace "<NA>" with None
            # df.replace("<NA>", None, inplace=True)

            # Use the existing database connection method
            conn = self.connect_to_database()

            # Create a cursor from the connection
            cursor = conn.cursor()

            # Get the column names from the SQL Server table
            cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableName}_View' ORDER BY ORDINAL_POSITION")
            columns = ', '.join([f'[{row[0]}]' for row in cursor.fetchall()])

            # Create an insert query
            query = f"INSERT INTO {tableName}_View ({columns}) VALUES ({', '.join('?' * len(df.columns))})"

            # Create a list of tuples from the DataFrame rows
            data = [tuple(row) for row in df.values]

            # Execute the query with the data
            cursor.executemany(query, data)

            # Get the number of rows in the DataFrame
            num_rows = len(df)

            # Print the number of rows imported
            print(f'{num_rows} rows were imported.')

            # Commit the changes
            conn.commit()
            logging.info(f"Pandas data from {file_path} inserted successfully.")

        except Exception as e:
            print(f'Pandas import failed: {e}')
        else:
            print('Pandas import succeeded')
        finally:
            # Close the cursor and connection
            cursor.close()
            conn.close()


    """
        Connects to a database based on the database type and credentials specified in the ETL configuration.

        This method constructs a connection string based on the database type, server, name, user ID, and password.
        If the database type is 'mssql', it uses the pyodbc library to connect to the SQL Server database. If the user ID 
        and password are provided, they are included in the connection string; otherwise, the connection string specifies 
        a trusted connection.

        If the database type is not 'mssql', it raises a ValueError.

        :return: A pyodbc.Connection object representing the database connection.
    
    """
    def connect_to_database(self):

       # Check if the database type is 'mssql'
        if self.db_type == 'mssql':
            # Construct a connection string for a SQL Server database using provided server and database names          
            conn_str = f'DRIVER={{SQL Server}};SERVER={self.dbServer};DATABASE={self.dbName};'
            
            # If user ID and password are provided, add them to the connection string
            if self.uid and self.pwd:
                conn_str += f'UID={self.uid};PWD={self.pwd}'
            else:
                # If user ID and password are not provided, use a trusted connection
                conn_str += 'Trusted_Connection=yes;'
            
            # Return a pyodbc connection object using the constructed connection string
            return pyodbc.connect(conn_str)
        else:
            # If the database type is not 'mssql', raise a ValueError
            raise ValueError("Unsupported database type")        


    """
        Processes a file based on its type and moves it to an archive folder after processing.

        This method checks the file type and calls the appropriate handler method to process the file.
        It supports CSV, TXT, and JSON file types. If the file is a ZIP file, it extracts the contents 
        before processing. After processing a file, it moves the file to an archive folder.

        If an error occurs while processing a file, it logs an error message and continues with the next file.
        If an error occurs while moving a file to the archive folder, it logs an error message.

        :param file_path: A string containing the path to the file to be processed.
        :param archive_path: A string containing the path to the archive folder where processed files should be moved.
        :param tableName: A string containing the name of the database table where the data should be imported.
    
    """
    def process_file(self, file_path, archive_path,tableName):

        # Define a dictionary mapping file types to their respective handler functions
        file_type_handlers = {
            'txt': self.handle_csv,
            'csv': self.handle_csv,
            'json': self.handle_json,
        }

        try:

            # Print the name of the file being processed, and if it's a zip file, print a message and extract its contents
            print(f"Processing file: {file_path}")
            if file_path.endswith('.zip'):
                print(f"Extracting file: {file_path}")
                self.extract_file_if_compressed(file_path)
            
            # Get the directory path of the file and print a message indicating the directory being processed
            directory_path = os.path.dirname(file_path)
            print(f"Processing files in directory: {directory_path}")

            # Iterate over all files in the directory and its subdirectories
            for root, dirs, files in os.walk(directory_path):
                for file in files:
                     # For each file, print a message indicating the file being processed
                    print(f"Processing file: {file}")

                    # If the file ends with the expected suffix and file type, print a message indicating a valid file was found
                    if file.endswith(self.file_suffix + '.' + self.file_type):
                        csv_file_path = os.path.join(root, file)   
                        print(f"Valid file found: {csv_file_path}")                   
                        
                        try:
                            # Process each file using the appropriate handler function for its file type
                            handler = file_type_handlers[self.file_type]
                            print(f"Processing {self.file_type} file: {csv_file_path}")
                            handler(csv_file_path,tableName)  

                            # Set drop_table_if_exists to False after processing a file
                            self.drop_table_if_exists = False

                            logging.info(f"Processed {self.file_type} file: {csv_file_path}")
                        except Exception as e:
                            # Log any exceptions that occur during file processing and continue with the next file
                            logging.error(f"Error processing file {csv_file_path}: {str(e)}")
                            continue  # Continue with the next file

                        # If the file was processed successfully, try to move it to the archive folder
                        try:
                            shutil.move(csv_file_path, os.path.join(archive_path, file))
                            logging.info(f"Moved {file} to the archive folder.")

                            # Set drop_table_if_exists to False after moving a file
                            self.drop_table_if_exists = False

                        except Exception as e:
                            # Log any exceptions that occur during file moving
                            logging.error(f"Error moving file {csv_file_path} to archive: {str(e)}")
                    else:
                         # If the file does not end with the expected suffix and file type, print a message indicating an invalid file was found
                        print(f"Invalid file found: {file}")
                        print(f"Expected file suffix: {self.file_suffix + '.' + self.file_type}")

        # Log any exceptions that occur during the execution of the process_file method
        except Exception as e:
            logging.error(f"Error in process_file method: {str(e)}")

    """
        Processes a CSV file and imports its data into a database table.

        This method reads the first row of the CSV file to get the column names, formats the column names for SQL, 
        and creates or updates the database table. It then imports the data from the CSV file into the database table 
        using either the BCP utility or pandas, depending on the ETL configuration.

        If an error occurs while processing the CSV file or importing the data, it logs an error message.

        :param file_path: A string containing the path to the CSV file to be processed.
        :param tableName: A string containing the name of the database table where the data should be imported.
    
    """
    def handle_csv(self, file_path, tableName):

        # Print a message indicating the CSV file being processed
        print(f"Processing CSV file: {file_path}")

        try:
            # Assign the field delimiter from the ETL configuration to a local variable
            delimiter = self.field_delimiter
           
            # If the CSV file has a header, read the column names from the first row
            if self.file_has_header :
                with open(file_path, 'r') as csvfile:
                    reader = csv.reader(csvfile, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
                     
                    # Format column names for SQL
                    columns = next(reader)
                    columns_sql = ['[' + column + '] varchar(max)' for column in columns]  # Format column names

                # Create or update table in the database
                self.create_table_and_view(columns_sql,tableName)

            # Print a message indicating the start of the data import process
            print(f"Importing data from {file_path} to {tableName}")
            try:
                # If BCP import is enabled, use it to import the data
                if self.bcp_import_bool:
                    self.bcp_import(file_path, tableName)
                # If pandas import is enabled, use it to import the data  
                elif self.pandas_import_bool:
                    self.pandas_import(file_path, tableName)
                # If no import method is selected, print a message 
                else:
                    print("No import method selected")
            except Exception as e:
                # Log any exceptions that occur during data import
                logging.error(f"Error importing data from {file_path} to {tableName}: {str(e)}")    
            
        except Exception as e:
            # Log any exceptions that occur during the processing of the CSV file
            logging.error(f"Error processing CSV file {file_path}: {str(e)}")   


    """
        Creates or verifies a database table and a corresponding view.

        This method connects to the database and creates a table with the specified columns. If the table already exists 
        and the drop_table_if_exists flag is True, it drops the table before creating it. It then creates a view that 
        includes all columns except the identity column.

        If an error occurs while creating or verifying the table or view, it logs an error message and rolls back the 
        transaction. After the operation, it closes the cursor and the database connection.

        :param columns_sql: A string or list containing the column definitions for the table.
        :param tableName: A string containing the name of the database table to be created or verified.
    
    """
    def create_table_and_view(self, columns_sql,tableName):      
        try:
            # Establish a connection to the database and create a cursor
            conn = self.connect_to_database()
            cursor = conn.cursor()            

            # Convert columns_sql to a list if it's a string
            if isinstance(columns_sql, str):
                columns_sql = columns_sql.split(', ')

            # If drop_table_if_exists is True, drop the table if it exists
            if self.drop_table_if_exists:
                drop_table_query = f"IF EXISTS (SELECT * FROM sys.tables WHERE name = N'{tableName}' AND type = 'U') DROP TABLE {tableName}"
                cursor.execute(drop_table_query)
                conn.commit()
                
                # Create a new table with the specified columns
                create_table_query = f"CREATE TABLE {tableName} (RecId INT PRIMARY KEY IDENTITY(1,1), {', '.join(columns_sql)})"  

                # Execute the create table query
                cursor.execute(create_table_query)
                conn.commit()
                logging.info(f"Table {tableName} created or verified successfully.")

                # Query the database for the column names of the table
                cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{tableName}' ORDER BY ORDINAL_POSITION")
                columns_sql = [row[0] for row in cursor.fetchall()]

                # Drop the view if it exists
                drop_view_query = f"IF EXISTS (SELECT * FROM sys.views WHERE name = N'{tableName}_View') DROP VIEW {tableName}_View"
                cursor.execute(drop_view_query)
                conn.commit()

                # Create a view that includes all columns except the identity column
                columns_without_id = [f"[{column}]" for column in columns_sql[1:]]  # Exclude the first column, which is the identity column
                create_view_query = f"CREATE VIEW {tableName}_View AS SELECT {', '.join(columns_without_id)} FROM {tableName}"
                cursor.execute(create_view_query)
                conn.commit()
                logging.info(f"View {tableName}_View created successfully.")

        except Exception as e:
            # Log any exceptions that occur during table or view creation
            logging.error(f"An error occurred while creating or verifying the table {tableName}: {e}")
            conn.rollback()  # Roll back the transaction

        finally:
            # Close the cursor and the connection
            cursor.close()
            conn.close()

    """
        Processes a JSON file and imports its data into a database table.

        This method connects to the database, opens the JSON file, and loads the data. It gets the column names 
        from the first item in the data, assumes all columns are of type NVARCHAR(MAX), and creates or verifies 
        the database table. It then inserts the JSON data into the database table.

        If an error occurs while processing the JSON file or importing the data, it logs an error message and rolls 
        back the transaction. After the operation, it closes the cursor and the database connection.

        :param file_path: A string containing the path to the JSON file to be processed.
        :param tableName: A string containing the name of the database table where the data should be imported.
    
    """
    def handle_json(self, file_path,tableName):
        try:
            # Establish a connection to the database and create a cursor
            conn = self.connect_to_database()
            cursor = conn.cursor()        

            # Open the JSON file and load the data into a Python object
            with open(file_path, 'r') as f:
                data = json.load(f)

            # Get the column names from the first item in the data
            columns = data[0].keys()

            # Assume all columns are of type NVARCHAR(MAX) for the SQL table
            columns_sql = ', '.join(f"[{column}] NVARCHAR(MAX)" for column in columns)

            # Create or update the table and view in the database
            self.create_table_and_view(columns_sql, tableName)

            # Iterate over each item in the data
            for item in data:
                # Get the column names and values from the item
                columns = ', '.join(item.keys())
                values = ', '.join(f"'{value}'" for value in item.values())

                # Construct an SQL insert query and execute it
                insert_query = f"INSERT INTO {tableName} ({columns}) VALUES ({values})"
                cursor.execute(insert_query)
            
            # Commit the transaction
            conn.commit()

            # Log a message indicating the successful insertion of the JSON data
            logging.info(f"JSON data from {file_path} inserted successfully.")

        except Exception as e:
            # Log any exceptions that occur during the processing of the JSON file
            logging.error(f"Error processing file {file_path}: {str(e)}")

            # Roll back the transaction in case of error
            conn.rollback()
        finally:
            # Close the cursor and the connection
            cursor.close()
            conn.close()
 
    """
        Processes data from URLs specified in the ETL configuration and imports the data into database tables.

        This method reads the download path, archive path, and file header flag from the ETL configuration. It also 
        reads the URL links, column names, and table names from the URL_SOURCE section of the configuration.

        For each URL, it downloads the data, checks if the file has a header, and if not, creates a table with the 
        specified columns. It then processes the downloaded file and moves it to the archive folder.

        If an error occurs while processing a URL, it logs an error message and continues with the next URL. If a 
        general error occurs, it logs an error message.

        :raises requests.exceptions.MissingSchema: If a URL is invalid.
    
    """
    def process_url(self):  
        try:
            # Get the download path, archive path, and file_has_header flag from the config file          
            downloadPath = self.config['ETL']['download_path']   
            archivePath = self.config['ETL']['archive_path']  
            file_has_header = self.config['ETL'].getboolean('file_has_header')                  

            # Get the URL links, column names, and table names from the config file
            url_links = self.config['URL_SOURCE']['url_links'].splitlines()
            url_column_names = self.config['URL_SOURCE']['url_column_names'].splitlines()
            url_table_names = self.config['URL_SOURCE']['url_table_names'].splitlines()

            # Process each URL
            for url, column_names, table_name in zip(url_links, url_column_names, url_table_names):
                if not url.strip():  # Skip empty URLs
                    continue
                try:
                    # Empty the folder of zip and csv files
                    self.empty_folder_of_zip_csv(downloadPath)

                    # Construct the file path and download the file from the URL               
                    file_path = os.path.join(downloadPath, f'{url.split("/")[-1]}')
                    self.download_from_url(url, file_path)

                    if not file_has_header:
                        # Assume all columns are of type NVARCHAR(MAX)
                        columns_sql = ', '.join(f"[{column}] NVARCHAR(MAX)" for column in column_names.split(',') if column.strip())    
                        
                        # Create the table and view in the database
                        self.create_table_and_view(columns_sql,table_name)
                    
                    # Print a message indicating the file being processed and process the file
                    print(f"Processing file: {file_path}")
                    self.process_file(file_path, archivePath,table_name)

                except requests.exceptions.MissingSchema:
                    # Log an error message if the URL is invalid
                    logging.error(f"Invalid URL: {url}")
                    continue

        except Exception as e:
            # Log any exceptions that occur during the processing of the URL
            logging.error(f"Error processing URL: {str(e)}")