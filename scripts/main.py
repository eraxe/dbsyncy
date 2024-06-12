# scripts/main.py
import os
import sys
import logging
import traceback
from dbsyncy_package import load_config, save_config, modify_config, sync_tables, setup_logging, setup_signal_handler
from dbsyncy_package.database import create_connection, create_new_connection
from dbsyncy_package.utils import get_tables, compress_and_copy_table
from termcolor import colored


def sync_hard_menu(config):
    while True:
        print("\n" + colored("Sync Database [Hard Sync]:", 'cyan', attrs=['bold']))
        print(colored("1. PUSH | local -> remote", 'blue'))
        print(colored("2. PULL | local <- remote", 'blue'))
        print(colored("3. SYNC | local <-> remote", 'blue'))
        print(colored("4. COMPRESS AND COPY | local -> remote if changes exceed threshold", 'blue'))
        print(colored("5. Back to Main Menu", 'blue'))
        choice = input(colored("Enter your choice: ", 'cyan', attrs=['bold']))

        if choice == "1":
            print(colored("\nPushing data from Local to Remote...", 'cyan', attrs=['bold']))
            try:
                sync_tables(
                    config=config,
                    direction='push',
                    batch_size=config["settings"]["batch_size"],
                    delete_missing=config["settings"]["delete_missing"],
                    dry_run=config["settings"]["dry_run"],
                    parallel=config["settings"]["parallel"]
                )
            except Exception as e:
                log_error("PUSH sync")
        elif choice == "2":
            print(colored("\nPulling data from Remote to Local...", 'cyan', attrs=['bold']))
            try:
                sync_tables(
                    config=config,
                    direction='pull',
                    batch_size=config["settings"]["batch_size"],
                    delete_missing=config["settings"]["delete_missing"],
                    dry_run=config["settings"]["dry_run"],
                    parallel=config["settings"]["parallel"]
                )
            except Exception as e:
                log_error("PULL sync")
        elif choice == "3":
            print(colored("\nSyncing data both ways...", 'cyan', attrs=['bold']))
            try:
                sync_tables(
                    config=config,
                    direction='both',
                    batch_size=config["settings"]["batch_size"],
                    delete_missing=config["settings"]["delete_missing"],
                    dry_run=config["settings"]["dry_run"],
                    parallel=config["settings"]["parallel"]
                )
            except Exception as e:
                log_error("SYNC")
        elif choice == "4":
            src_connection = create_connection(**config["local"], pool_name="src_pool",
                                               pool_size=config["settings"]["pool_size"])
            dest_connection = create_connection(**config["remote"], pool_name="dest_pool",
                                                pool_size=config["settings"]["pool_size"])
            try:
                for table in get_tables(src_connection):
                    compress_and_copy_table(src_connection, dest_connection, table,
                                            threshold=config["settings"]["threshold"])
            except KeyError as ke:
                print(colored(f"Missing configuration key: {ke}", 'red'))
                logging.error(f"Missing configuration key: {ke}")
            except Exception as e:
                log_error("COMPRESS AND COPY")
            finally:
                src_connection.close()
                dest_connection.close()
        elif choice == "5":
            break
        else:
            print(colored("Invalid choice. Please try again.", 'red', attrs=['bold']))


def log_error(context):
    exc_type, exc_value, exc_traceback = sys.exc_info()
    tb = traceback.format_exception(exc_type, exc_value, exc_traceback)
    error_message = f"Error during {context}: {exc_type.__name__} - {exc_value}\n" + "".join(tb)
    print(colored(error_message, 'red'))
    logging.error(error_message)


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, '..', 'config.json')

    config = load_config(config_path)
    setup_logging()

    while True:
        print("\n" + colored("Select an option:", 'cyan', attrs=['bold']))
        print(colored("1. Sync Database [Hard Sync]", 'blue'))
        print(colored("2. Sync Database [Soft Sync]", 'blue'))
        print(colored("3. Sync Database Structure", 'blue'))
        print(colored("4. Settings", 'blue'))
        print(colored("9. Exit", 'blue'))
        choice = input(colored("Enter your choice: ", 'cyan', attrs=['bold']))

        if choice == "1":
            sync_hard_menu(config)
        elif choice == "2":
            sync_soft_menu(config)
        elif choice == "3":
            sync_structure_menu(config)
        elif choice == "4":
            modify_config(config)
        elif choice == "9":
            print(colored("Exiting...", 'cyan', attrs=['bold']))
            break
        else:
            print(colored("Invalid choice. Please try again.", 'red', attrs=['bold']))

    if 'src_connection' in globals() and src_connection:
        src_connection.close()
    if 'dest_connection' in globals() and dest_connection:
        dest_connection.close()


if __name__ == "__main__":
    main()
