import datetime
import json
import os
import logging
from typing import Dict, Optional

class OrderLogger:
    def __init__(self, log_file: str = "order_log.jsonl"):
        """
        Initialize the logger with a file path.
        Default log file is 'order_log.jsonl' in the current directory.
        Uses JSON Lines format for easy appending and parsing.
        Ensures the log directory exists.
        """
        self.log_file = log_file
        log_dir = os.path.dirname(self.log_file)
        if log_dir and not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir, exist_ok=True)
            except Exception as e:
                logging.error(f"Failed to create log directory {log_dir}: {e}")
        # Ensure file exists
        if not os.path.exists(self.log_file):
            try:
                with open(self.log_file, "w") as f:
                    pass
            except Exception as e:
                logging.error(f"Failed to create log file {self.log_file}: {e}")

    def log_order(self, order_details: Dict, timestamp: Optional[datetime.datetime] = None) -> None:
        """
        Log order details as a JSON object with timestamp.

        Args:
            order_details (Dict): Dictionary containing order info.
            timestamp (datetime, optional): Custom timestamp. If None, uses current time.
        """
        try:
            order_details = dict(order_details)  # Make a copy to avoid mutating input
            order_details["timestamp"] = (timestamp or datetime.datetime.now()).isoformat()
            with open(self.log_file, "a") as f:
                f.write(json.dumps(order_details) + "\n")
        except Exception as e:
            logging.error(f"Failed to log order: {e}. Order details: {order_details}")
