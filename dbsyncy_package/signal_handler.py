# dbsyncy_package/signal_handler.py
import signal
import sys

def signal_handler(sig, frame):
    print('Process terminated. Closing connections...')
    if 'src_connection' in globals() and src_connection:
        src_connection.close()
    if 'dest_connection' in globals() and dest_connection:
        dest_connection.close()
    sys.exit(0)

def setup_signal_handler():
    signal.signal(signal.SIGINT, signal_handler)
