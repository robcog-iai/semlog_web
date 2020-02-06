
from datetime import datetime
import os


class Logger:
    def __init__(self, log_path):
        self.log_path = os.path.join(log_path, "log.txt")

    def write(self, msg, flag_print=True):
        """Write one msg to log file. Add time and line break."""
        file = open(self.log_path, "a")
        current_time = "["+datetime.now().strftime("%b %d %Y %H:%M:%S")+"]"
        log_msg = current_time + "  " + msg + "@" +"\n" 
        file.write(log_msg)
        if flag_print is True:
            print(log_msg)

    def read(self):
        file = open(self.log_path, "r")
        return file.read()