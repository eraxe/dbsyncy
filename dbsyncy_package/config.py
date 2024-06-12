# dbsyncy_package/config.py
import json
import logging
import traceback
import sys
from termcolor import colored

def log_error():
    exc_type, exc_value, exc_traceback = sys.exc_info()
    tb = traceback.format_exception(exc_type, exc_value, exc_traceback)
    error_message = f"{exc_type.__name__} - {exc_value}\n" + "".join(tb)
    print(colored(error_message, 'red'))
    logging.error(error_message)

def load_config(config_file):
    try:
        with open(config_file, 'r') as file:
            config = json.load(file)
        return config
    except Exception as e:
        log_error()
        return {}

def save_config(config_file, config):
    try:
        with open(config_file, 'w') as file:
            json.dump(config, file, indent=4)
    except Exception as e:
        log_error()

def modify_config(config):
    try:
        while True:
            print("\n" + colored("Modify Configuration:", 'cyan', attrs=['bold']))
            print(colored("1. Change Local Database Settings", 'blue'))
            print(colored("2. Change Remote Database Settings", 'blue'))
            print(colored("3. Change Sync Settings", 'blue'))
            print(colored("4. Back to Main Menu", 'blue'))
            choice = input(colored("Enter your choice: ", 'cyan', attrs=['bold']))

            if choice == "1":
                for key in config["local"]:
                    new_value = input(colored(f"Enter new value for {key} (current: {config['local'][key]}): ", 'cyan'))
                    if new_value:
                        config["local"][key] = new_value
            elif choice == "2":
                for key in config["remote"]:
                    new_value = input(colored(f"Enter new value for {key} (current: {config['remote'][key]}): ", 'cyan'))
                    if new_value:
                        config["remote"][key] = new_value
            elif choice == "3":
                for key in config["settings"]:
                    new_value = input(colored(f"Enter new value for {key} (current: {config['settings'][key]}): ", 'cyan'))
                    if new_value:
                        config["settings"][key] = type(config["settings"][key])(new_value)  # Preserve type
            elif choice == "4":
                break
            else:
                print(colored("Invalid choice. Please try again.", 'red', attrs=['bold']))

            save_config('config.json', config)
            print(colored("Configuration updated successfully!", 'green'))
    except Exception as e:
        log_error()
