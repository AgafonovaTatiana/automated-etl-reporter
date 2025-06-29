#!/usr/bin/env python
# coding: utf-8

import requests
import psycopg2
from datetime import datetime, timedelta
import ast
import logging
import os
import glob
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import smtplib
import ssl
from email.message import EmailMessage
import json
import sys

#Все "чувствительные данные" (логины, пароли, данные для подключения по API) сохранены в файле config.json

CONFIG_FILE = '/Path/to/your/config/file/config.json'
class ConfigLoader:
    """
    Loads configuration from a JSON file.
    Handles errors related to file not found, JSON decoding, and other exceptions.
    """
    def __init__(self, config_path=CONFIG_FILE):
        self.config_path = config_path
        self.config = self.load_config()
        
    def load_config(self):
        
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            return config
        except FileNotFoundError:
            logging.error(f"Configuration file not found: {CONFIG_FILE}")
            return None
        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON from configuration file: {CONFIG_FILE}")
            return None
        except Exception as e:
            logging.error(f"An unexpected error occurred loading config: {e}")
            return None

#Во время обработки будет сохраняться лог работы скрипта с отлавливанием всех ошибок и выводом промежуточных стадий 
#(например, скачивание началось / скачивание завершилось / заполнение базы началось и т.д., с трекингом времени). 
#Лог сохраняется в текстовый файл. Файл именуется в соответствии с текущей датой. 
#Если в папке с логами уже есть другие логи - они удаляются, остаются только логи за последние 3 дня.

class LoggerManager:
    """
    Manages logging setup and cleanup.
    Sets up logging to a file based on the current date and cleans up logs older than 3 days.
    """
    def __init__(self, log_dir='logs', keep_days=3):
        self.log_dir = log_dir
        self.keep_days = keep_days
        self._setup_logging()

    def _setup_logging(self):
        """
        Sets up logging to a file based on the current date.
        """
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
        
        current_date_str = datetime.now().strftime('%Y-%m-%d')
        log_file_path = os.path.join(self.log_dir, f'{current_date_str}.log')
        
        self._cleanup_old_logs()

        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        
        logging.basicConfig(
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
            level=logging.INFO, 
            handlers=[logging.FileHandler(log_file_path, 'a', 'utf-8')]
            )

    def _cleanup_old_logs(self):
        """
        Deletes log files in the specified directory older than days_to_keep.
        Assumes log files are named in YYYY-MM-DD.log format.
        """
        now = datetime.now()
        for file in glob.glob(os.path.join(self.log_dir, '*.log')):
            file_date_str = os.path.basename(file).split('.')[0]
            try:
                file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
                if now - file_date > timedelta(days=self.keep_days):
                    os.remove(file)
            except ValueError:
                logging.warning(f"Skipping log file with unexpected name format: {file}")
            except Exception as e:
                logging.error(f"Error deleting log file {file}: {e}")



logger_manager = LoggerManager(log_dir='logs', keep_days=3)
logging.info("Log file cleanup finished.")

config_loader = ConfigLoader(CONFIG_FILE)
config = config_loader.config
if config is None or \
   not config.get('database') or \
   not config.get('email') or \
   not config.get('google_sheets') or \
   not config.get('api_config'): 
    logging.error("Failed to load essential configuration sections. Exiting.")
    sys.exit(1)

db_params = config.get('database')
email_config = config.get('email')
google_sheets_config = config.get('google_sheets')
api_config = config.get('api_config')
api_client = api_config.get('client')
api_client_key = api_config.get('client_key')
#Для дальнейшей автоматизации выгрузки понадобится автоматическая подстановка дат, 
#сейчас же можно будет обойтись ручным вводом
#end_time = datetime.now()
#start_time = end_time - timedelta(hours=1
#formatted_start_time = start_time.strftime('%Y-%m-%d %H:%M:%S.%f')
#formatted_end_time = end_time.strftime('%Y-%m-%d %H:%M:%S.%f')
formatted_start_time = '2023-04-01 12:46:47.860798'
formatted_end_time = '2023-04-01 13:46:47.860798'

def fetch_api_data(api_url, params):
    """
    Fetches data from a given API endpoint with parameters.
    """
    logging.info(f"Attempting to fetch data from API: {api_url}")
    try:
        r = requests.get(api_url, params=params)
        r.raise_for_status()
        api_response_data = r.json()
        logging.info("Data fetching completed successfully.")
        
        if isinstance(api_response_data, list):
             logging.info(f"Fetched {len(api_response_data)} records.")
             return api_response_data
        elif api_response_data is None:
             logging.warning("API response was None.")
             return [] 
        else:
             logging.warning(f"API response was not a list, type: {type(api_response_data)}")
             return [] 
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return [] 
    except Exception as e: 
        logging.error(f"An unexpected error occurred during API data processing: {e}")
        return []    


def connect_to_database(db_params):
    """
    Establishes a connection to the PostgreSQL database.
    """
    conn = None
    cur = None
    try:
        logging.info("Attempting to connect to the database.")
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        logging.info("Database connection successful.")
        return conn, cur
    except psycopg2.OperationalError as e:
        logging.error(f"Database connection failed: {e}")
        return None, None
    except Exception as e:
        logging.error(f"Error during database connection: {e}")
        return None, None


def create_data_table(conn, cur):
    """
    Checks if 'data' table exists and creates it if necessary.
    """
    if not conn or not cur:
        logging.error("Database connection or cursor not available for table creation.")
        return

    try:
        cur.execute("""
            SELECT EXISTS (
               SELECT 1
               FROM pg_tables
               WHERE tablename = 'data'
               );
            """)
        
        table_exists = cur.fetchone()[0]
    
        if table_exists:
            logging.info("Table 'data' already exists. Skipping table creation.")
        else:
            logging.info("Table 'data' does not exist. Creating table.")
        
        cur.execute("""
            CREATE TABLE data (
                id SERIAL PRIMARY KEY,
                user_id character varying(50) NOT NULL,
                oauth_consumer_key character varying(20),
                lis_result_sourcedid character varying(255) NOT NULL,
                lis_outcome_service_url character varying(255) NOT NULL,
                is_correct int,
                attempt_type character varying(10) NOT NULL,
                created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL
            )
        """)
        conn.commit()
        logging.info("Table 'data' created successfully.")
     
    except Exception as e:
        logging.error(f"Error during table creation: {e}")
        if conn:
            conn.rollback()

#При занесении данных в БД, проверяется все ли ячейки, кроме 'oauth_consumer_key', заполнены.
#Если данные отсутствуют, строка в БД не заносится, ошибка занесения записывается в файл логов.

def insert_data_into_db(conn, cur, api_data):
    """
    Inserts a list of data records into the 'data' table.
    """
    if not conn or not cur:
        logging.error("Database connection or cursor not available for data insertion.")
        return

    inserted_count = 0
    skipped_count = 0
    try:
        for el in api_data:
            try:
                data_dict = el
                passback_params_str = data_dict.get('passback_params', '{}')
                try:
                    passback_params_dict = ast.literal_eval(passback_params_str)
                    if not isinstance(passback_params_dict, dict):
                         logging.warning(f"passback_params did not evaluate to a dictionary: {passback_params_str}")
                         passback_params_dict = {} 
                
                except (ValueError, SyntaxError) as e:
                     logging.error(f"Error evaluating passback_params: {e} for data: {passback_params_str}")
                     passback_params_dict = {}     
                
                is_correct_val = data_dict.get('is_correct')
                if data_dict.get('attempt_type') == 'run':
                    is_correct_val = None
                        
                extracted_tuple = (
                  data_dict.get('lti_user_id'),
                  passback_params_dict.get('oauth_consumer_key'),
                  passback_params_dict.get('lis_result_sourcedid'),
                  passback_params_dict.get('lis_outcome_service_url'),
                  is_correct_val,
                  data_dict.get('attempt_type'),
                  data_dict.get('created_at')
                )
                query = """
                INSERT INTO data (
                    user_id, oauth_consumer_key, lis_result_sourcedid,
                    lis_outcome_service_url, is_correct, attempt_type, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """    
                cur.execute(query, extracted_tuple)
                conn.commit()
                inserted_count += 1
            except psycopg2.Error as e:
                logging.error(f"Database error inserting line: {e} for data: {el}")
                if conn:
                    conn.rollback() 
                skipped_count += 1
        
        logging.info(f"Data insertion complete: {inserted_count} lines inserted, {skipped_count} lines skipped.")
 
    except psycopg2.Error as e:
        print(f"Database error during insertion: {e}")
        if conn:
            conn.rollback() 
        logging.error(f"Database error during insertion: {e}")

#Загрузка отчетных данных в таблицу Google Sheets:
#При автоматизации скрипта (запуск каждый день в определенное время, выгрузка данных за день, сохранение лога, #формирование отчета) в SQL код можно будет добавить фильтр по дате, чтобы отчет предоставлялся на текущую дату.
#Либо можно будет организовать Google таблицу следующим образом: каждый день будет добавляться новый столбец с текущей датой и туда будут заноситься данные по текущему дню.

def get_report_statistics(conn, cur):
    """Queries the database for report statistics."""
    if not conn or not cur:
        logging.error("Database connection or cursor not available for report statistics.")
        return None, None, None

    try:
        logging.info("Creating daily report.")
        
        cur.execute("""SELECT COUNT(attempt_type) FROM data
                        WHERE attempt_type = 'submit'""")
        attempts_amount = cur.fetchone()[0]
        
        cur.execute("""SELECT COUNT(attempt_type) FROM data
                        WHERE attempt_type = 'submit'
                        AND is_correct = 1""")
        successful_attempts = cur.fetchone()[0]
        
        cur.execute("""SELECT COUNT(DISTINCT user_id) FROM data""")
        distinct_users = cur.fetchone()[0]

        return attempts_amount, successful_attempts, distinct_users

    except psycopg2.Error as e:
        logging.error(f"Database error during report queries: {e}")
        if conn:
            conn.rollback() 
        return None, None, None
    except Exception as e: 
        logging.error(f"An unexpected error occurred in get_report_statistics: {e}")
        
        if conn: conn.rollback()
        return None, None, None


def update_google_sheet(attempts_amount, successful_attempts, distinct_users, creds_path=google_sheets_config.get('creds_path'), sheet_name=google_sheets_config.get('sheet_name')):
    """
    Updates the Google Sheet with report statistics.
    """
    try:
        scope = ['https://www.googleapis.com/auth/spreadsheets.readonly',
                 'https://www.googleapis.com/auth/drive']
        gc = gspread.service_account(filename=creds_path)
        sh = gc.open(sheet_name)

        sh.sheet1.update_cell(1,2,attempts_amount)
        sh.sheet1.update_cell(2,2,successful_attempts)
        sh.sheet1.update_cell(3,2,distinct_users)
        
        logging.info("Daily report created successfully.")
    
    except Exception as e:
        logging.error(f"Error updating Google Sheet: {e}")

#Отправка информационного сообщения на почту

def send_completion_email(sender, password, recipient, subject, message):
    """
    Sends an email notification.
    """
    try:
        context = ssl.create_default_context()
        smtp_server = 'smtp.mail.ru'
        port = 465
        
        msg = EmailMessage()
        msg.set_content(message)
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = recipient
        
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(sender, password)
            server.send_message(msg)
        logging.info("Email sent successfully.")

    except Exception as e:
        logging.error(f"Error sending email: {e}")

logging.info("Script started.")

api_url = api_config.get('url')
params = {
    'client': api_client,
    'client_key': api_client_key,
    'start': formatted_start_time,
    'end': formatted_end_time
}

api_data = fetch_api_data(api_url, params)

conn, cur = connect_to_database(db_params)

if conn and cur:
    try:    
        create_data_table(conn, cur)
        insert_data_into_db(conn, cur, api_data)
    finally:
        if cur: cur.close()
        if conn: conn.close()
    conn_report, cur_report = connect_to_database(db_params)
    if conn_report and cur_report:
        try:
            report_stats = get_report_statistics(conn_report, cur_report)
            
            if cur_report: cur_report.close()
            if conn_report: conn_report.close()

            if report_stats is not None and all(stat is not None for stat in report_stats):
                attempts, successful, distinct = report_stats
                update_google_sheet(attempts, successful, distinct, 
                                    creds_path=google_sheets_config.get('creds_path'),
                                    sheet_name=google_sheets_config.get('sheet_name'))
            else:
                logging.error("Report statistics could not be retrieved due to a database error.")
        finally:
            if cur_report: cur_report.close()
            if conn_report: conn_report.close()
    else:
            logging.error("Could not establish connection for reporting.")
else:
        logging.error("Database operations skipped due to connection failure.")


email_sender = email_config.get('sender')
email_password = email_config.get('password')
email_recipient = email_config.get('recipient')
email_subject = "Загрузка данных"
email_message = f"""Добрый день,

Автоматический скрипт успешно завершил выполнение.
Период данных в отчете: с {formatted_start_time} по {formatted_end_time}.
Подробный отчет о работе скрипта и все зафиксированные события доступны для просмотра в файле логов.

С уважением,
Татьяна""" 

send_completion_email(email_sender, email_password, email_recipient, email_subject, email_message)

logging.info("Script finished.")