#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""

Description:

# This script defines the main function for an ETL (Extract, Transform, Load) process focused on handling data from an SFTP source.
# It performs the following steps:
# 1. Initializes the ETL process with a configuration file specific to SFTP sources. Users need to replace the path with their local configuration file path.
# 2. Initializes the AWS SSM client to securely retrieve the SFTP password, specifying the AWS region. Users should replace the region with their own.
# 3. Retrieves the SFTP password from AWS SSM Parameter Store.
# 4. Prepares the download folder by removing any existing zip and csv files.
# 5. Downloads the specified file from the SFTP server to the local download path.
# 6. Processes the downloaded file by:
#    a. Extracting and transforming the data as needed.
#    b. Loading the transformed data into a specified MSSQL database table.
#    c. Archiving the processed file to a specified archive path.
# 7. Calculates the total execution time of the ETL process.
# 8. Sends an email notification upon successful completion or failure of the ETL process.
# 9. Logs the outcome of the ETL process.
# Note: Users must ensure that the AWS SSM, SFTP, and MSSQL configurations are correctly set in the configuration file.


"""

from etlModule import EmailUtility
from etlModule import ETLProcess

from datetime import datetime
import logging
import os
import boto3


logging.basicConfig(level=logging.INFO)

def main():

    start_time = datetime.now()
    # Initialize the ETLProcess with a configuration file for SFTP sources. Replace 'e:\ETLsolutions\config_sftp.ini' with the path to your local configuration file.
    etl = ETLProcess('e:\ETLsolutions\config_sftp.ini')
    # Initialize the AWS SSM client with your AWS region. Replace 'us-west-2' with your AWS region.  
    ssm = boto3.client('ssm', region_name='us-west-2')  
    sftp_password = ssm.get_parameter(Name='sftp_password', WithDecryption=True)['Parameter']['Value']       

    try:          
        downloadPath = etl.config['ETL']['download_path']   
        etl.empty_folder_of_zip_csv(downloadPath)

        filename =etl.config['ETL']['file_name']   
        archivePath =etl.config['ETL']['archive_path'] 
        tableName = etl.config['MSSQL']['table_name']         
        targetFile=os.path.join(downloadPath,filename)  

        etl.download_from_sftp(etl.config['SFTP_SOURCE']['host'], etl.config['SFTP_SOURCE']['port'],
                etl.config['SFTP_SOURCE']['username'], sftp_password,
                etl.config['SFTP_SOURCE']['remote_path'], downloadPath)
 
        etl.process_file(targetFile,archivePath,tableName)
         
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        etl.email_util.send_email("ETL Process Successful", f"The ETL process completed successfully in {execution_time} seconds.")
        logging.info("ETL process completed successfully.")

    except Exception as e:
        logging.error(f"ETL process failed: {str(e)}")
        etl.email_util.send_email("ETL Process Failed", f"ETL process failed with error: {str(e)}")


if __name__ == "__main__":
    main()


