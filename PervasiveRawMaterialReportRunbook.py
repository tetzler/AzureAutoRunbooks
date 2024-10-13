# This Python script automates the generation and distribution of a raw material inventory report.
# It connects to a Pervasaive SQL database, fetches inventory data, exports it to an Excel file, saves the file
# to a network folder on dc01, and sends a notification email. The script uses pyodbc for database
# connection, pandas for data manipulation, and smtplib for email notifications. It also includes
# error handling and logging functionality.
import pyodbc
import pandas as pd
import os
import logging
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from contextlib import closing

# Configuration
CONFIG = {
    'database': {
        'CYS': {
            'dsn': 'GLOBAL_CYS64',
            'uid': 'Master',
            'pwd': 'master'
        }
    },
    'email': {
        'smtp_server': "spamfilter.highergroundtech.com",
        'smtp_port': 25,
        'sender_email': "cytech@circley.com",
        'recipients': ["travis@kasparcompanies.com"]
    },
    'network_folder': r'\\v-dc01\shares\user data\05 Shared Reports\Auto RM Report',
    'log_file': 'auto_rm_report.log'
}

# Setup logging
logging.basicConfig(
    filename=CONFIG['log_file'],
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def connect_to_database(company_code):
    db_config = CONFIG['database'].get(company_code)
    connection_string = f"DSN={db_config['dsn']};UID={db_config['uid']};PWD={db_config['pwd']};"
    return pyodbc.connect(connection_string)

def fetch_data(conn):
    query = """
    SELECT 
        v.PART,
        v.DESCRIPTION,
        v.LOCATION,
        v.CODE_SORT,
        v.PRODUCT_LINE,
        v.QTY_ONHAND,
        v.QTY_REQUIRED,
        (v.QTY_ONHAND - v.QTY_REQUIRED) AS QTY_DIFFERENCE,
        v.amt_Cost,
        (v.amt_Cost * v.QTY_ONHAND) as cost_extended
    FROM 
        v_inventory_mstr v
    JOIN 
        prodline_mre p
    ON 
        v.PRODUCT_LINE = p.prod_line
    WHERE 
        p.type = 'RM'
    ORDER BY 
        v.PART ASC;
    """
    return pd.read_sql(query, conn)

def generate_filename():
    return f"raw_material_inv_report_{datetime.now():%m%d%y_%H%M}.xlsx"

def export_to_excel(dataframe, filepath):
    try:
        dataframe.to_excel(filepath, index=False)
        logging.info("Excel file saved successfully at %s", filepath)
    except Exception as e:
        logging.error("Error saving Excel file: %s", e)

def send_notification_email(filepath):
    email_config = CONFIG['email']
    msg = MIMEMultipart()
    msg['From'] = email_config['sender_email']
    msg['To'] = ", ".join(email_config['recipients'])
    msg['Subject'] = "Auto RM Inv Report - Successfully saved to file share"
    body_content = f"The raw material inventory report was successfully saved to the file share at the following location:\n\n{filepath}"
    msg.attach(MIMEText(body_content, 'plain'))

    try:
        with smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port']) as server:
            server.sendmail(email_config['sender_email'], email_config['recipients'], msg.as_string())
        logging.info("Notification email sent successfully!")
    except Exception as e:
        logging.error("Error sending notification email: %s", e)

def main():
    try:
        with closing(connect_to_database('CYS')) as conn:
            data = fetch_data(conn)
            filename = generate_filename()
            network_excel_path = os.path.join(CONFIG['network_folder'], filename)
            
            os.makedirs(CONFIG['network_folder'], exist_ok=True)
            export_to_excel(data, network_excel_path)
            send_notification_email(network_excel_path)
    except Exception as e:
        logging.error("An error occurred in the main function: %s", e)

if __name__ == "__main__":
    main()