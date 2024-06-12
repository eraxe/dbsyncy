# dbsyncy_package/sync.py
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from termcolor import colored
import traceback
import sys
from .utils import batch, prepare_row
from .database import get_changed_rows, compare_and_sync_structure, create_new_connection, get_existing_columns, get_table_structure

def log_error():
    exc_type, exc_value, exc_traceback = sys.exc_info()
    tb = traceback.format_exception(exc_type, exc_value, exc_traceback)
    error_message = f"{exc_type.__name__} - {exc_value}\n" + "".join(tb)
    print(colored(error_message, 'red'))
    logging.error(error_message)

def sync_rows(src_connection, dest_connection, table_name, changed_rows, deleted_rows, delete_missing, batch_size=100,
              dry_run=False, parallel=False):
    try:
        existing_columns = get_existing_columns(dest_connection, table_name)
        table_structure = get_table_structure(dest_connection, table_name)
        cursor = dest_connection.cursor()

        def process_chunk(chunk):
            values_list = []
            for row in chunk:
                row = prepare_row(row, existing_columns, table_structure)
                columns = ', '.join(row.keys())
                values = ', '.join([f"{value}" for value in row.values()])
                update_clause = ', '.join([f"{key}=VALUES({key})" for key in row.keys()])
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values}) ON DUPLICATE KEY UPDATE {update_clause}"
                retry_count = 0
                while retry_count < 3:
                    try:
                        if not dry_run:
                            cursor.execute(sql)
                            logging.info(f"Inserted/Updated row in table {table_name}: {values}")
                        break
                    except Error as e:
                        if 'Lock wait timeout exceeded' in str(e):
                            retry_count += 1
                            logging.warning(f"Lock wait timeout exceeded. Retrying {retry_count}/3")
                            time.sleep(1)
                        else:
                            print(colored(f"Error synchronizing table {table_name}: {e}", 'red'))
                            logging.error(f"Error synchronizing table {table_name}: {e}")
                            break

        if parallel:
            with ThreadPoolExecutor() as executor:
                futures = [executor.submit(process_chunk, chunk) for chunk in batch(changed_rows, batch_size)]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as exc:
                        log_error()
        else:
            for chunk in batch(changed_rows, batch_size):
                process_chunk(chunk)

        if delete_missing:
            def process_delete(row):
                conditions = ' AND '.join([f"{key}='{value}'" for key, value in row.items() if value != 'NULL'])
                sql = f"DELETE FROM {table_name} WHERE {conditions}"
                retry_count = 0
                while retry_count < 3:
                    try:
                        if not dry_run:
                            cursor.execute(sql)
                            logging.info(f"Deleted row from {table_name}: {conditions}")
                        break
                    except Error as e:
                        if 'Lock wait timeout exceeded' in str(e):
                            retry_count += 1
                            logging.warning(f"Lock wait timeout exceeded. Retrying {retry_count}/3")
                            time.sleep(1)
                        else:
                            print(colored(f"Error deleting row from table {table_name}: {e}", 'red'))
                            logging.error(f"Error deleting row from table {table_name}: {e}")
                            break

            if parallel:
                with ThreadPoolExecutor() as executor:
                    futures = [executor.submit(process_delete, row) for row in batch(deleted_rows, batch_size)]
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as exc:
                            log_error()
            else:
                for row in batch(deleted_rows, batch_size):
                    process_delete(row)

        if not dry_run:
            dest_connection.commit()
        print(colored(f"Table {table_name} synchronized", 'green'))
        logging.info(f"Table {table_name} synchronized")
    except Exception as e:
        log_error()

def sync_tables(config, direction='both', batch_size=100, delete_missing=True, dry_run=False, parallel=False):
    global src_connection, dest_connection
    try:
        src_config = config["local"] if direction == 'push' else config["remote"]
        dest_config = config["remote"] if direction == 'push' else config["local"]

        src_connection = create_new_connection(src_config)
        dest_connection = create_new_connection(dest_config)

        if not src_connection or not dest_connection:
            print(colored("Failed to create initial connections", 'red'))
            return

        src_tables = get_tables(src_connection)
        dest_tables = get_tables(dest_connection)
        src_connection.close()
        dest_connection.close()

        common_tables = list(set(src_tables) & set(dest_tables))

        if parallel:
            with ThreadPoolExecutor() as executor:
                futures = [executor.submit(process_table, config, table, direction, batch_size,
                                           delete_missing, dry_run, parallel) for table in common_tables]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as exc:
                        log_error()
        else:
            for table in tqdm(common_tables, desc="Syncing Tables", unit="table"):
                process_table(config, table, direction, batch_size, delete_missing, dry_run, parallel)
    except Exception as e:
        log_error()

def process_table(config, table, direction, batch_size, delete_missing, dry_run, parallel):
    try:
        src_config = config["local"] if direction in ['push', 'both'] else config["remote"]
        dest_config = config["remote"] if direction in ['push', 'both'] else config["local"]

        src_connection = create_new_connection(src_config)
        dest_connection = create_new_connection(dest_config)

        if not src_connection or not dest_connection:
            print(colored(f"Failed to create connection for table {table}", 'red'))
            return

        if direction in ['push', 'both']:
            if has_table_changed(src_connection, dest_connection, table):
                compare_and_sync_structure(src_connection, dest_connection, table)
                changed_rows, deleted_rows = get_changed_rows(src_connection, dest_connection, table)
                sync_rows(src_connection, dest_connection, table, changed_rows, deleted_rows, delete_missing, batch_size,
                          dry_run, parallel)

        if direction in ['pull', 'both']:
            if has_table_changed(dest_connection, src_connection, table):
                compare_and_sync_structure(dest_connection, src_connection, table)
                changed_rows, deleted_rows = get_changed_rows(dest_connection, src_connection, table)
                sync_rows(dest_connection, src_connection, table, changed_rows, deleted_rows, delete_missing, batch_size,
                          dry_run, parallel)

        src_connection.close()
        dest_connection.close()
    except Exception as e:
        log_error()

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

def get_table_row_count(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        result = cursor.fetchone()
        return result[0]
    except Exception as e:
        log_error()
        return 0

def get_row_checksum(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"CHECKSUM TABLE {table_name}")
        result = cursor.fetchone()
        return result[1]
    except Exception as e:
        log_error()
        return 0
