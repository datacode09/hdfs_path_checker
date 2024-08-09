import os
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

HDFS_PATHS = [
    'hdfs://prod-ns:8020/prod/01559/app/RIEQ/data.ide/ATOMDataFiles/Productivity/CAG5BR/Outputs/',
    # Add more paths here as needed
]

# Create logs directory if it doesn't exist
script_dir = os.path.dirname(os.path.realpath(__file__))
logs_dir = os.path.join(script_dir, 'logs')
os.makedirs(logs_dir, exist_ok=True)

# Configure logging with date-time stamp in log file name
log_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")
log_filepath = os.path.join(logs_dir, log_filename)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', 
                    handlers=[logging.FileHandler(log_filepath), logging.StreamHandler()])

def create_spark_session():
    try:
        spark = SparkSession.builder.appName('HDFSCheckerApp').getOrCreate()
        logging.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
        raise

def extract_datetime_from_filename(file_name):
    # Define possible datetime patterns
    datetime_patterns = [
        r'\d{8}_\d{6}',        # YYYYMMDD_HHMMSS
        r'\d{8}T\d{6}',        # YYYYMMDDTHHMMSS (ISO 8601)
        r'\d{8}',              # YYYYMMDD
        r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
        r'\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}', # YYYY-MM-DD_HH-MM-SS
    ]
    
    for pattern in datetime_patterns:
        match = re.search(pattern, file_name)
        if match:
            datetime_str = match.group()
            try:
                # Attempt to parse different datetime formats
                if '_' in datetime_str:
                    dt = datetime.strptime(datetime_str, '%Y%m%d_%H%M%S')
                elif 'T' in datetime_str:
                    dt = datetime.strptime(datetime_str, '%Y%m%dT%H%M%S')
                elif '-' in datetime_str and '_' in datetime_str:
                    dt = datetime.strptime(datetime_str, '%Y-%m-%d_%H-%M-%S')
                elif '-' in datetime_str:
                    dt = datetime.strptime(datetime_str, '%Y-%m-%d')
                else:
                    dt = datetime.strptime(datetime_str, '%Y%m%d')
                return dt
            except ValueError as e:
                logging.warning(f"Failed to parse datetime from {datetime_str}: {e}")
    
    logging.info(f"No datetime found in filename: {file_name}")
    return None

def get_parquet_row_count(spark, hdfs_file_path):
    try:
        df = spark.read.parquet(hdfs_file_path)
        row_count = df.count()
        return row_count
    except Exception as e:
        logging.error(f"Error getting row count for {hdfs_file_path}: {e}")
        return None

def parse_hdfs_ls_output(spark, line):
    parts = line.split()
    modification_date = parts[5]
    modification_time = parts[6]
    file_path = parts[-1]
    
    modification_datetime = datetime.strptime(f"{modification_date} {modification_time}", "%Y-%m-%d %H:%M")
    file_name = os.path.basename(file_path)
    file_datetime = extract_datetime_from_filename(file_name)
    row_count = get_parquet_row_count(spark, file_path)
    
    return modification_datetime, file_name, file_datetime, row_count

def check_files_in_directory(spark, hdfs_path, start_time, end_time):
    try:
        cmd = f'hdfs dfs -ls {hdfs_path}'
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            logging.error(f"Error running HDFS command: {result.stderr.strip()}")
            return []

        file_infos = result.stdout.strip().splitlines()
        modified_files_info = []

        for file_info in file_infos:
            is_modified, file_name, file_datetime, row_count = parse_hdfs_ls_output(spark, file_info)
            if start_time <= is_modified <= end_time:
                modified_files_info.append({
                    "file_name": file_name,
                    "file_datetime": file_datetime,
                    "row_count": row_count,
                })

        return modified_files_info
    except Exception as e:
        logging.error(f"Error checking files in directory {hdfs_path}: {e}")
        return []

def generate_html_table(data):
    html = '<table border="1">'
    html += '<tr><th>HDFS Path</th><th>Status</th><th>Modified Files</th><th>Row Count</th></tr>'
    for row in data:
        html += '<tr>'
        html += f'<td>{row[0]}</td>'
        html += f'<td>{row[1]}</td>'
        for file_info in row[2]:
            html += f'<td>{file_info["file_name"]}</td>'
            html += f'<td>{file_info["row_count"]}</td>'
        html += '</tr>'
    html += '</table>'
    return html

def send_email(subject, message):
    try:
        sender = "sender@example.com"
        recipients = ["recipient@example.com"]
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = ", ".join(recipients)
        
        msg.attach(MIMEText(message, 'html'))

        with smtplib.SMTP('smtp.example.com') as server:
            server.login('user', 'password')
            server.sendmail(sender, recipients, msg.as_string())
            logging.info("Email sent successfully.")
    except Exception as e:
        logging.error(f"Error sending email: {e}")

def main():
    try:
        spark = create_spark_session()

        today = datetime.now()
        start_time = datetime.combine(today, datetime.min.time()) + timedelta(hours=0)  # 12:00 AM
        end_time = datetime.combine(today, datetime.min.time()) + timedelta(hours=6)    # 6:00 AM

        paths_status = []
        all_paths_modified = True

        for hdfs_path in HDFS_PATHS:
            modified_files_info = check_files_in_directory(spark, hdfs_path, start_time, end_time)
            if modified_files_info:
                paths_status.append([hdfs_path, 'Modified', modified_files_info])
            else:
                paths_status.append([hdfs_path, 'Not Modified', ''])
                all_paths_modified = False

        table = generate_html_table(paths_status)
        
        if all_paths_modified:
            subject = "✔️ [Success] All HDFS Paths Modified"
            message = f"<p>All HDFS paths have been modified between 12:00 AM and 6:00 AM.</p>{table}"
        else:
            subject = "❌ [Failure] Some HDFS Paths Not Modified"
            message = f"<p>Some HDFS paths have not been modified between 12:00 AM and 6:00 AM.</p>{table}"

        logging.info(message)
        send_email(subject, message)
        spark.stop()
    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")

if __name__ == "__main__":
    main()
