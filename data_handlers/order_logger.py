import datetime
import json
import logging
from pathlib import Path
from typing import Dict, Optional, Any
from contextlib import contextmanager

# Configure logging
logger = logging.getLogger(__name__)


class OrderLogger:
    """
    Handles logging of trading order details in JSON Lines format.
    Provides thread-safe logging with proper error handling and validation.
    """
    
    # Constants
    DEFAULT_LOG_FILE = "order_log.jsonl"
    
    def __init__(self, log_file: Optional[str] = None):
        """
        Initialize the OrderLogger with a file path.
        
        Args:
            log_file: Path to the log file. If None, uses default location.
                     Uses JSON Lines format for easy appending and parsing.
        """
        self.log_file = Path(log_file) if log_file else Path(self.DEFAULT_LOG_FILE)
        self._ensure_log_directory()
        self._ensure_log_file_exists()
    
    # Public methods (alphabetically ordered)
    
    def get_log_file_path(self) -> Path:
        """
        Get the current log file path.
        
        Returns:
            Path: Current log file path
        """
        return self.log_file
    
    def log_order(self, order_details: Dict[str, Any], timestamp: Optional[datetime.datetime] = None) -> bool:
        """
        Log order details as a JSON object with timestamp.

        Args:
            order_details: Dictionary containing order information
            timestamp: Custom timestamp. If None, uses current time

        Returns:
            bool: True if logging successful, False otherwise
            
        Raises:
            ValueError: If order_details is invalid
        """
        if not self._validate_order_details(order_details):
            raise ValueError("Invalid order details provided")
        
        try:
            # Create a copy to avoid mutating the input
            order_data = dict(order_details)
            order_data["timestamp"] = (timestamp or datetime.datetime.now()).isoformat()
            
            with self._managed_file_write() as file_handle:
                file_handle.write(json.dumps(order_data, default=str) + "\n")
                
            logger.debug("Successfully logged order: %s", order_data.get("order_id", "unknown"))
            return True
            
        except Exception as e:
            logger.error("Failed to log order: %s. Order details: %s", e, order_details)
            return False
    
    def read_orders(self, limit: Optional[int] = None) -> list[Dict[str, Any]]:
        """
        Read logged orders from the file.
        
        Args:
            limit: Maximum number of orders to read. If None, reads all.
            
        Returns:
            List of order dictionaries
        """
        orders = []
        
        if not self.log_file.exists():
            logger.warning("Log file does not exist: %s", self.log_file)
            return orders
            
        try:
            with open(self.log_file, 'r', encoding='utf-8') as file:
                for line_num, line in enumerate(file, 1):
                    if limit and len(orders) >= limit:
                        break
                        
                    line = line.strip()
                    if not line:
                        continue
                        
                    try:
                        order = json.loads(line)
                        orders.append(order)
                    except json.JSONDecodeError as e:
                        logger.warning("Invalid JSON on line %d: %s", line_num, e)
                        
        except Exception as e:
            logger.error("Failed to read orders from %s: %s", self.log_file, e)
            
        logger.info("Read %d orders from log file", len(orders))
        return orders
    
    # Private methods (alphabetically ordered)
    
    def _ensure_log_directory(self) -> None:
        """Ensure the log directory exists."""
        log_dir = self.log_file.parent
        
        if log_dir != Path('.') and not log_dir.exists():
            try:
                log_dir.mkdir(parents=True, exist_ok=True)
                logger.info("Created log directory: %s", log_dir)
            except Exception as e:
                logger.error("Failed to create log directory %s: %s", log_dir, e)
                raise
    
    def _ensure_log_file_exists(self) -> None:
        """Ensure the log file exists."""
        if not self.log_file.exists():
            try:
                self.log_file.touch()
                logger.info("Created log file: %s", self.log_file)
            except Exception as e:
                logger.error("Failed to create log file %s: %s", self.log_file, e)
                raise
    
    @contextmanager
    def _managed_file_write(self):
        """Context manager for safe file writing operations."""
        file_handle = None
        try:
            file_handle = open(self.log_file, 'a', encoding='utf-8')
            yield file_handle
        except Exception as e:
            logger.error("Error writing to log file %s: %s", self.log_file, e)
            raise
        finally:
            if file_handle:
                file_handle.close()
    
    def _validate_order_details(self, order_details: Dict[str, Any]) -> bool:
        """
        Validate order details before logging.
        
        Args:
            order_details: Order details dictionary to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not isinstance(order_details, dict):
            logger.error("Order details must be a dictionary")
            return False
            
        if not order_details:
            logger.error("Order details cannot be empty")
            return False
            
        # Check for required fields (basic validation)
        required_fields = ["order_type"]
        for field in required_fields:
            if field not in order_details:
                logger.warning("Missing recommended field in order details: %s", field)
                
        return True