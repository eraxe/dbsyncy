# dbsyncy_package/utils.py
from itertools import islice
import logging
import csv
import shutil
import sys
import traceback
from termcolor import colored

def log_error():
    exc_type, exc_value, exc_traceback = sys.exc_info()
    tb = traceback.format_exception(exc_type, exc_value, exc_traceback)
    error_message = f"{exc_type.__name__} - {exc_value}\n" + "".join(tb)
    print(colored(error_message, 'red'))
    logging.error(error_message)

def batch(iterable, n=1):
    it = iter(iterable)
    while True:
        chunk = tuple(islice(it, n))
        if not chunk:
            return
        yield chunk

def prepare_row(row, existing_columns, table_structure):
    prepared_row = {}
    for key in row:
        if key in existing_columns:
            value = row[key]
            column_type = table_structure.get(key, '').lower()
            if value is None:
                prepared_row[key] = 'NULL'
            elif 'int' in column_type:
                try:
                    prepared_row[key] = int(value)
                except ValueError:
                    logging.error(f"Invalid int value for column '{key}': {value}")
                    prepared_row[key] = 'NULL'
            elif 'datetime' in column_type or 'timestamp' in column_type:
                try:
                    if isinstance(value, str) and not re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', value):
                        logging.error(f"Invalid datetime value format for column '{key}': {value}")
                        prepared_row[key] = 'NULL'
                    else:
                        prepared_row[key] = f"'{value}'"
                except ValueError:
                    logging.error(f"Invalid datetime value for column '{key}': {value}")
                    prepared_row[key] = 'NULL'
            elif 'float' in column_type or 'double' in column_type or 'decimal' in column_type:
                try:
                    prepared_row[key] = float(value)
                except ValueError:
                    logging.error(f"Invalid float value for column '{key}': {value}")
                    prepared_row[key] = 'NULL'
            elif 'text' in column_type or 'char' in column_type:
                prepared_row[key] = f"'{str(value).replace("'", "''")}'"
            else:
                prepared_row[key] = f"'{str(value).replace("'", "''")}'"
    return prepared_row

def get_row_checksum(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"CHECKSUM TABLE {table_name}")
        result = cursor.fetchone()
        return result[1]
    except Exception as e:
        log_error()
        return 0

def get_table_row_count(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        result = cursor.fetchone()
        return result[0]
    except Exception as e:
        log_error()
        return 0

def has_table_changed(src_connection, dest_connection, table_name):
    try:
        src_row_count = get_table_row_count(src_connection, table_name)
        dest_row_count = get_table_row_count(dest_connection, table_name)

        if src_row_count != dest_row_count:
            return True

        src_checksum = get_row_checksum(src_connection, table_name)
        dest_checksum = get_row_checksum(dest_connection, table_name)

        return src_checksum != dest_checksum
    except Exception as e:
        log_error()
        return False

def export_csv(connection, table_name, file_name):
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()

        with open(file_name, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        print(colored(f"Exported {len(rows)} rows from {table_name} to {file_name}", 'green'))
    except Exception as e:
        log_error()

def import_csv(connection, table_name, file_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"""
            LOAD DATA LOCAL INFILE '{file_name}'
            INTO TABLE {table_name}
            FIELDS TERMINATED BY ',' 
            OPTIONALLY ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES;
        """)
        connection.commit()
        print(colored(f"Imported data from {file_name} into {table_name}", 'green'))
    except Exception as e:
        log_error()

def compress_and_copy_table(src_connection, dest_connection, table_name, threshold=10000):
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

        if len(changed_rows) > threshold:
            print(colored(f"Compressing and copying {table_name} with {len(changed_rows)} changed rows...", 'cyan'))
            logging.info(f"Compressing and copying {table_name} with {len(changed_rows)} changed rows")

            export_file = 'source_table.csv'
            export_csv(src_connection, table_name, export_file)
            shutil.copy(export_file, 'destination_table.csv')
            import_csv(dest_connection, table_name, 'destination_table.csv')
            os.remove(export_file)
            os.remove('destination_table.csv')

            print(colored(f"Table {table_name} compressed and copied successfully", 'green'))
            logging.info(f"Table {table_name} compressed and copied successfully")
        else:
            print(colored(f"Table {table_name} changes are below threshold, skipping compression and copy", 'yellow'))
            logging.info(f"Table {table_name} changes are below threshold, skipping compression and copy")
    except Exception as e:
        log_error()

def get_existing_columns(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()
        return {column[0] for column in columns}
    except Exception as e:
        log_error()
        return set()

def get_table_structure(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()
        table_structure = {column[0]: column[1] for column in columns}
        return table_structure
    except Exception as e:
        log_error()
        return {}

def get_table_collation(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"SHOW TABLE STATUS LIKE '{table_name}'")
        result = cursor.fetchone()
        return result[14]  # Collation is the 15th column in the SHOW TABLE STATUS result
    except Exception as e:
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
    except Exception as e:
        log_error()
        return None

def get_tables(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        return [table[0] for table in tables]
    except Exception as e:
        log_error()
        return []
