import os
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from py4j.java_gateway import java_import
import smtplib
from email.mime.text import MIMEText

HDFS_PATHS = [
    'hdfs://prod-ns:8020/prod/01559/app/RIEQ/data.ide/ATOMDataFiles/Productivity/CAG5BR/Outputs/',
    # Add more paths here as needed
]

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', 
                    handlers=[logging.FileHandler("hdfs_path_checker.log"), logging.StreamHandler()])

def create_spark_session():
    try:
        spark = SparkSession.builder.appName('HDFSCheckerApp').getOrCreate()
        logging.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
        raise

def get_filesystem_manager(spark):
    try:
        java_import(spark._jvm, 'org.apache.hadoop.fs.FileSystem')
        java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.FileSystem.get(hadoop_conf)
        logging.info("Filesystem manager created successfully.")
        return fs
    except Exception as e:
        logging.error(f"Error creating filesystem manager: {e}")
        raise

def check_file_modification_time(hdfs_path, start_time, end_time, fs):
    try:
        path = spark._jvm.Path(hdfs_path)
        file_status = fs.getFileStatus(path)
        
        modification_time = file_status.getModificationTime()
        modification_time = datetime.utcfromtimestamp(modification_time / 1000)
        
        if start_time <= modification_time <= end_time:
            return True
        return False
    except Exception as e:
        logging.error(f"Error checking file modification time for {hdfs_path}: {e}")
        return False

def check_files_in_directory(hdfs_path, start_time, end_time, fs):
    try:
        path = spark._jvm.Path(hdfs_path)
        file_statuses = fs.listStatus(path)
        modified_files = []
        
        for status in file_statuses:
            file_path = status.getPath().toString()
            if check_file_modification_time(file_path, start_time, end_time, fs):
                modified_files.append(file_path)
        
        return modified_files
    except Exception as e:
        logging.error(f"Error checking files in directory {hdfs_path}: {e}")
        return []

def send_email(subject, message):
    try:
        sender = "sender@example.com"
        recipients = ["recipient@example.com"]
        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = ", ".join(recipients)

        with smtplib.SMTP('smtp.example.com') as server:
            server.login('user', 'password')
            server.sendmail(sender, recipients, msg.as_string())
            logging.info("Email sent successfully.")
    except Exception as e:
        logging.error(f"Error sending email: {e}")

def main():
    try:
        spark = create_spark_session()
        fs = get_filesystem_manager(spark)
        
        today = datetime.now()
        start_time = datetime.combine(today, datetime.min.time()) + timedelta(hours=0) # 12:00 AM
        end_time = datetime.combine(today, datetime.min.time()) + timedelta(hours=6)  # 6:00 AM

        for hdfs_path in HDFS_PATHS:
            modified_files = check_files_in_directory(hdfs_path, start_time, end_time, fs)
            if modified_files:
                message = f"Modified files in {hdfs_path} between 12:00 AM and 6:00 AM: {modified_files}"
                logging.info(message)
                send_email(f"Files Modified in {hdfs_path}", message)
            else:
                logging.info(f"No files modified in {hdfs_path} between 12:00 AM and 6:00 AM.")
        
        spark.stop()
    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")

if __name__ == "__main__":
    main()
