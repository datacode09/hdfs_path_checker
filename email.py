import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Email parameters
EMAIL_TO = "recipient@example.com"
EMAIL_FROM = "sender@example.com"
SMTP_SERVER = "smtp.example.com"
SMTP_PORT = 587
SMTP_USER = "your_smtp_username"
SMTP_PASS = "your_smtp_password"
SUBJECT = "HDFS Report"

# Read the HTML report generated by the Bash script
with open('/tmp/hdfs_report.html', 'r') as file:
    html_content = file.read()

# Create the email
msg = MIMEMultipart()
msg['From'] = EMAIL_FROM
msg['To'] = EMAIL_TO
msg['Subject'] = SUBJECT
msg.attach(MIMEText(html_content, 'html'))

# Send the email via SMTP
with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
    server.starttls()
    server.login(SMTP_USER, SMTP_PASS)
    server.send_message(msg)

print("Email sent successfully!")
