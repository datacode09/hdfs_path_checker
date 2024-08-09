import os
import logging
import subprocess
from datetime import datetime, timedelta
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

def run_hdfs_ls(hdfs_path):
    try:
        cmd = f'hdfs dfs -ls {hdfs_path}'
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            logging.error(f"Error running HDFS command: {result.stderr.strip()}")
            return None
        return result.stdout.strip()
    except Exception as e:
        logging.error(f"Error running shell command for {hdfs_path}: {e}")
        return None

def check_file_modification_time(file_info, start_time, end_time):
    try:
        # Extract the modification time from the file_info
        parts = file_info.split()
        modification_time_str = f"{parts[5]} {parts[6]}"
        modification_time = datetime.strptime(modification_time_str, "%Y-%m-%d %H:%M")
        
        if start_time <= modification_time <= end_time:
            return True
        return False
    except Exception as e:
        logging.error(f"Error checking file modification time: {e}")
        return False

def check_files_in_directory(hdfs_path, start_time, end_time):
    try:
        output = run_hdfs_ls(hdfs_path)
        if not output:
            return []

        file_infos = output.splitlines()
        modified_files = []

        for file_info in file_infos:
            if check_file_modification_time(file_info, start_time, end_time):
                file_path = file_info.split()[-1]
                modified_files.append(file_path)

        return modified_files
    except Exception as e:
        logging.error(f"Error checking files in directory {hdfs_path}: {e}")
        return []

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

def generate_html_table(data):
    html = '<table border="1">'
    html += '<tr><th>HDFS Path</th><th>Status</th><th>Modified Files</th></tr>'
    for row in data:
        html += '<tr>'
        html += f'<td>{row[0]}</td>'
        html += f'<td>{row[1]}</td>'
        html += f'<td>{row[2]}</td>'
        html += '</tr>'
    html += '</table>'
    return html

def main():
    try:
        today = datetime.now()
        start_time = datetime.combine(today, datetime.min.time()) + timedelta(hours=0) # 12:00 AM
        end_time = datetime.combine(today, datetime.min.time()) + timedelta(hours=6)  # 6:00 AM

        paths_status = []
        all_paths_modified = True

        for hdfs_path in HDFS_PATHS:
            modified_files = check_files_in_directory(hdfs_path, start_time, end_time)
            if modified_files:
                paths_status.append([hdfs_path, 'Modified', ', '.join(modified_files)])
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
    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")

if __name__ == "__main__":
    main()
