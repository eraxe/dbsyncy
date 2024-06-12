# dbsyncy_package/database.py
import mysql.connector
from mysql.connector import Error, pooling
from termcolor import colored
import logging
import traceback
import sys

def log_error():
    exc_type, exc_value, exc_traceback = sys.exc_info()
    tb = traceback.format_exception(exc_type, exc_value, exc_traceback)
    error_message = f"{exc_type.__name__} - {exc_value}\n" + "".join(tb)
    print(colored(error_message, 'red'))
    logging.error(error_message)

def create_connection(host_name, user_name, user_password, db_name, pool_name=None, pool_size=5):
    try:
        if pool_name:
            connection = pooling.MySQLConnectionPool(
                pool_name=pool_name,
                pool_size=pool_size,
                pool_reset_session=True,
                host=host_name,
                user=user_name,
                password=user_password,
                database=db_name,
                client_flags=[mysql.connector.ClientFlag.LOCAL_FILES],
                allow_local_infile=True
            ).get_connection()
        else:
            connection = mysql.connector.connect(
                host=host_name,
                user=user_name,
                passwd=user_password,
                database=db_name,
                client_flags=[mysql.connector.ClientFlag.LOCAL_FILES],
                allow_local_infile=True
            )
        print(colored(f"Connection to {db_name} DB successful", 'green'))
        logging.info(f"Connected to {db_name} database")
        return connection
    except Error as e:
        log_error()
        return None

def create_new_connection(config):
    try:
        connection = mysql.connector.connect(
            host=config["host"],
            user=config["user"],
            passwd=config["password"],
            database=config["database"],
            client_flags=[mysql.connector.ClientFlag.LOCAL_FILES],
            allow_local_infile=True
        )
        return connection
    except Error as e:
        log_error()
        return None

def get_primary_key(connection, table_name):
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(f"""
            SELECT k.COLUMN_NAME
            FROM information_schema.table_constraints t
            JOIN information_schema.key_column_usage k
            USING(constraint_name,table_schema,table_name)
            WHERE t.constraint_type='PRIMARY KEY'
                AND t.table_schema=DATABASE()
                AND t.table_name='{table_name}';
        """)
        result = cursor.fetchone()
        if result:
            return result['COLUMN_NAME']
        else:
            raise ValueError(f"No primary key found for table {table_name}")
    except Error as e:
        log_error()
        return None

def get_existing_columns(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()
        return {column[0] for column in columns}
    except Error as e:
        log_error()
        return None

def get_table_structure(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()
        table_structure = {column[0]: column[1] for column in columns}
        return table_structure
    except Error as e:
        log_error()
        return None

def get_table_collation(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"SHOW TABLE STATUS LIKE '{table_name}'")
        result = cursor.fetchone()
        return result[14]  # Collation is the 15th column in the SHOW TABLE STATUS result
    except Error as e:
        log_error()
        return None

def check_and_create_table(src_connection, dest_connection, table_name):
    try:
        cursor = dest_connection.cursor()
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        result = cursor.fetchone()
        if not result:
            print(colored(f"Table {table_name} does not exist on remote. Creating...", 'yellow'))
            table_schema = get_table_schema(src_connection, table_name)
            create_table(dest_connection, table_schema)
        else:
            sync_table_collation(src_connection, dest_connection, table_name)
    except Error as e:
        log_error()

def get_table_schema(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"SHOW CREATE TABLE {table_name}")
        result = cursor.fetchone()
        return result[1]
    except Error as e:
        log_error()
        return None

def create_table(connection, table_schema):
    try:
        cursor = connection.cursor()
        cursor.execute(table_schema)
        connection.commit()
        print(colored("Table created successfully", 'green'))
        logging.info("Table created successfully")
    except Error as e:
        log_error()

def sync_table_collation(src_connection, dest_connection, table_name):
    try:
        src_collation = get_table_collation(src_connection, table_name)
        dest_collation = get_table_collation(dest_connection, table_name)
        if src_collation != dest_collation:
            cursor = dest_connection.cursor()
            cursor.execute(f"ALTER TABLE {table_name} CONVERT TO CHARACTER SET utf8mb4 COLLATE {src_collation}")
            dest_connection.commit()
            print(colored(f"Updated table collation for {table_name} to {src_collation}", 'green'))
            logging.info(f"Updated table collation for {table_name} to {src_collation}")
    except Error as e:
        log_error()

def compare_and_sync_structure(src_connection, dest_connection, table_name):
    try:
        src_structure = get_table_structure(src_connection, table_name)
        dest_structure = get_table_structure(dest_connection, table_name)

        for column, src_column_type in src_structure.items():
            if column not in dest_structure:
                cursor = dest_connection.cursor()
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column} {src_column_type}")
                print(colored(f"Added column {column} to {table_name} in destination", 'green'))
                logging.info(f"Added column {column} to {table_name} in destination")
            elif src_column_type != dest_structure[column]:
                cursor = dest_connection.cursor()
                cursor.execute(f"ALTER TABLE {table_name} MODIFY COLUMN {column} {src_column_type}")
                print(colored(f"Modified column {column} in {table_name} in destination", 'green'))
                logging.info(f"Modified column {column} in {table_name} in destination")
            else:
                sync_column_collation(src_connection, dest_connection, table_name, column)
    except Error as e:
        log_error()

def sync_column_collation(src_connection, dest_connection, table_name, column):
    try:
        cursor = src_connection.cursor()
        cursor.execute(f"SHOW FULL COLUMNS FROM {table_name} LIKE '{column}'")
        src_collation = cursor.fetchone()[2]

        cursor = dest_connection.cursor()
        cursor.execute(f"SHOW FULL COLUMNS FROM {table_name} LIKE '{column}'")
        dest_collation = cursor.fetchone()[2]

        if src_collation != dest_collation:
            cursor.execute(f"ALTER TABLE {table_name} MODIFY {column} COLLATE {src_collation}")
            dest_connection.commit()
            print(colored(f"Updated column collation for {column} in {table_name} to {src_collation}", 'green'))
            logging.info(f"Updated column collation for {column} in {table_name} to {src_collation}")
    except Error as e:
        log_error()

def get_changed_rows(src_connection, dest_connection, table_name):
    try:
        primary_key = get_primary_key(src_connection, table_name)

        cursor = src_connection.cursor(dictionary=True)
        cursor.execute(f"SELECT * FROM {table_name}")
        src_rows = cursor.fetchall()

        cursor = dest_connection.cursor(dictionary=True)
        cursor.execute(f"SELECT * FROM {table_name}")
        dest_rows = cursor.fetchall()

        src_dict = {row[primary_key]: row for row in src_rows}
        dest_dict = {row[primary_key]: row for row in dest_rows}

        changed_rows = [src_dict[key] for key in src_dict if key not in dest_dict or src_dict[key] != dest_dict[key]]
        deleted_rows = [dest_dict[key] for key in dest_dict if key not in src_dict]

        return changed_rows, deleted_rows
    except Error as e:
        log_error()
        return [], []
