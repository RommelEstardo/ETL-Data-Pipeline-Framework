#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""

    Description:

    # This script defines the main function for an ETL (Extract, Transform, Load) process that operates on local data sources.
    # It uses a configuration file to specify paths and settings, handling data extraction, transformation, and loading into a database.
    # The process includes:
    # - Emptying a specified folder of its contents (assumed to be zip and csv files).
    # - Processing a specified file from a local source directory.
    # - Archiving the processed file.
    # - Loading the data into a specified database table.
    # Execution time is calculated, and email notifications are sent upon successful completion or failure of the ETL process.

"""

from etlModule import EmailUtility
from etlModule import ETLProcess

from datetime import datetime
import logging
import os

logging.basicConfig(level=logging.INFO)

def main():

    start_time = datetime.now()
    # Initialize the ETLProcess with a configuration file. Replace 'e:\ETLsolutions\config_local.ini' with your local path to the config file.
    etl = ETLProcess('e:\ETLsolutions\config_local.ini')  

    try:          
        downloadPath = etl.config['ETL']['download_path']   
        etl.empty_folder_of_zip_csv(downloadPath)

        filename = etl.config['ETL']['file_name']   
        archivePath = etl.config['ETL']['archive_path']  
        tableName = etl.config['MSSQL']['table_name']          

        folderPath = etl.config['LOCAL_SOURCE']['folder_path']   
        print(folderPath, tableName)  
        etl.process_file(folderPath,archivePath,tableName)
         
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        etl.email_util.send_email("ETL Process Successful", f"The ETL process completed successfully in {execution_time} seconds.")
        logging.info("ETL process completed successfully.")

    except Exception as e:
        logging.error(f"ETL process failed: {str(e)}")
        etl.email_util.send_email("ETL Process Failed", f"ETL process failed with error: {str(e)}")


if __name__ == "__main__":
    main()

