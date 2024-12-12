#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Description:

# This script defines an ETL (Extract, Transform, Load) process specifically for handling data from URLs.
# It sets up logging to capture informational messages and errors.
# The main function initializes the ETL process with a configuration file, processes data from a specified URL,
# calculates the execution time, and sends an email notification upon completion or failure.

"""

from etlModule import EmailUtility
from etlModule import ETLProcess

from datetime import datetime
import logging
import os
import requests

logging.basicConfig(level=logging.INFO)

def main():

    start_time = datetime.now()
    # Initialize the ETLProcess with a configuration file. Replace 'e:\ETLsolutions\config_url.ini' with your local path to the config file.
    etl = ETLProcess('e:\ETLsolutions\config_url.ini')  

    try:          
        etl.process_url()       
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        etl.email_util.send_email("ETL Process Successful", f"The ETL process completed successfully in {execution_time} seconds.")
        logging.info("ETL process completed successfully.")

    except Exception as e:
        logging.error(f"ETL process failed: {str(e)}")
        etl.email_util.send_email("ETL Process Failed", f"ETL process failed with error: {str(e)}")


if __name__ == "__main__":
    main()

