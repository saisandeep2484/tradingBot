# config_template.py
# Copy this file to config.py and fill in your actual credentials

API_KEY = "your_api_key_here"
API_SECRET = "your_api_secret_here"
REQUEST_TOKEN = "your_request_token_here"

# CAUTION: The exchange in the stock symbol must be included in the EXCHANGES list above
EXCHANGES = ["your_exchange1_here", "your_exchange2_here"]  # List of exchanges to connect to - multiple exchanges can be added - currently only NSE and BSE are supported
INTRADAY_TARGET_STOCK = "your_exchange:your_symbol_here"  # Format: "exchange:ticker" - works with any stock (e.g., "NSE:RELIANCE", "BSE:TCS", etc.)
                                   

