
from datetime import datetime
import os


class Logger:
    def __init__(self, log_path,log_file_name=None):
        self.log_path = os.path.join(log_path, log_file_name+".log")

    def write(self, msg, flag_print=True):
        """Write one msg to log file. Add time and line break."""
        file = open(self.log_path, "a")
        insert_time=datetime.now().strftime('%H:%M:%S.%f')[:-3]
        current_time = "["+insert_time+"]"
        log_msg = current_time + "  " + msg + "$" +"\n" 
        file.write(log_msg)
        # if flag_print is True:
        print(log_msg)

    def read(self):
        file = open(self.log_path, "r")
        return file.read()
