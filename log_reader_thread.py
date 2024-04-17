import logging
import os
import queue
import threading
import time
from datetime import datetime


class LogReaderThread(threading.Thread):
    """
    Thread to tail log files and send the job_id to a FIFO queue
    """

    def __init__(self, q: queue.Queue, config: dict, logger: logging.Logger, *args, **kwargs):
        """
        Constructor
        :param q: safe-threading FIFO queue
        :param config: configuration settings
        :param logger: logger
        :param args: args
        :param kwargs: kwargs
        """
        self.q = q
        self.log_delay = config["log_file_generation_delay_seconds"]
        self.knime_logs_path = config["knime_logs_path"]
        self.logger = logger
        super().__init__(*args, **kwargs)

    def get_filename(self) -> str:
        """
        Get log filename based on today's date
        :return: knime server log filename by today's date
        """
        return os.path.join(self.knime_logs_path, f"localhost.{datetime.today().strftime('%Y-%m-%d')}.log")

    @staticmethod
    def extract_job_id(line: str) -> str:
        """
        Extract job_id from log line. It's the 8th last word in lines that ends with
        EXECUTION_FAILED (failed execution) or EXECUTION_FINISHED (success execution)
        :param line: individual line from log
        :return: job_id if present, "" if no job_id
        """
        if "EXECUTION_FAILED" in line or "EXECUTION_FINISHED" in line:
            # Get job_id from the 8th last word. It is in format (job_id) so remove parenthesis
            return line.split()[-8].replace("(", "").replace(")", "")
        else:
            return ""

    def run(self) -> None:
        """
        Thread infinite run to tail log files and send job_id to the queue to be processed in main.py.
        Log files changes every day as the name of the file contains the current day, this is handled here.
        To ensure that all the previous file is read when a new log file appears at 00:00, a delay is applied before
        start reading the new file.
        """
        previous_filename = self.get_filename()  # Start with an initial value
        # Store file pointer to avoid reading already read lines. Start from end of file.
        with open(previous_filename) as f:
            f.read()  # Must read current log file to update file pointer
            file_position = f.tell()  # Set file pointer to end of file
        current_delay = None
        while True:
            filename = self.get_filename()

            # Check if new filename is triggered (change of day, so new log file created)
            if previous_filename != filename:
                if not os.path.isfile(filename):
                    # The new day file does not exist yet, so stay in yesterday's log file
                    filename = previous_filename
                else:
                    # Apply some delay ('log_delay' seconds) to ensure the previous file is entirely read
                    if current_delay is None:
                        current_delay = datetime.now()
                        filename = previous_filename  # stick to previous file
                    else:
                        if (datetime.now() - current_delay).total_seconds() > self.log_delay:
                            # Here the filename has the new one, we are not forcing the previous_filename
                            file_position = 0  # Start pointer again for new file
                            current_delay = None  # Reset delay
                        else:
                            filename = previous_filename    # stick to previous file

            try:
                # Tail log file
                with open(filename, 'r') as f:
                    f.seek(file_position)  # fast-forward beyond content read previously
                    for line in f:
                        job_id = self.extract_job_id(line.strip())
                        if job_id != "":
                            # Only send finished jobs ids
                            self.logger.info(f"Encountered job {job_id}")
                            self.q.put(job_id)
                    file_position = f.tell()  # store position at which to resume
            except IOError as ex:
                self.logger.error(f"Error reading {filename}. Exception: {ex}")

            # Store filename that has just been processed
            previous_filename = filename
            # Save CPU usage
            time.sleep(0.5)
