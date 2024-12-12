#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""

Description:

# This script defines the main function for an ETL (Extract, Transform, Load) process focused on handling data from an S3 source.
# It performs the following steps:
# 1. Initializes the ETL process with a configuration file specific to S3 sources.
# 2. Empties the specified download folder of any existing zip and csv files to prepare for new downloads.
# 3. Downloads data from a specified S3 bucket and folder to the local download path.
# 4. Iterates over each file in the download path, processing each file by:
#    a. Extracting and transforming the data as needed.
#    b. Loading the transformed data into a specified MSSQL database table.
#    c. Archiving the processed file to a specified archive path.
#    d. Emptying the download folder again to prepare for the next file's processing.
# 5. Calculates the total execution time of the ETL process.
# 6. Sends an email notification upon successful completion or failure of the ETL process.
# 7. Logs the outcome of the ETL process.

"""

from etlModule import EmailUtility
from etlModule import ETLProcess

from datetime import datetime
import logging
import os


logging.basicConfig(level=logging.INFO)

def main():

    start_time = datetime.now()
    # Initialize the ETLProcess with a configuration file for S3 sources. Replace 'e:\ETLsolutions\config_s3.ini' with the path to your local configuration file.
    etl = ETLProcess('e:\ETLsolutions\config_s3.ini')  

    try:          
        downloadPath = etl.config['ETL']['download_path']   
        etl.empty_folder_of_zip_csv(downloadPath)

        filename = etl.config['ETL']['file_name']   
        archivePath = etl.config['ETL']['archive_path']
        tableName = etl.config['MSSQL']['table_name']          
        targetFile=os.path.join(downloadPath,filename)  

        etl.download_from_s3(etl.config['S3_SOURCE']['s3_bucket'], etl.config['S3_SOURCE']['s3_folder'], downloadPath)
        files = os.listdir(downloadPath)

        for filename in files:
            targetFile = os.path.join(downloadPath, filename)
            etl.process_file(targetFile, archivePath, tableName)
            etl.empty_folder_of_zip_csv(downloadPath)
         
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        etl.email_util.send_email("ETL Process Successful", f"The ETL process completed successfully in {execution_time} seconds.")
        logging.info("ETL process completed successfully.")

    except Exception as e:
        logging.error(f"ETL process failed: {str(e)}")
        etl.email_util.send_email("ETL Process Failed", f"ETL process failed with error: {str(e)}")


if __name__ == "__main__":
    main()

