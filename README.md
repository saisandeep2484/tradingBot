# Trading Bot

A Python-based algorithmic trading bot for Indian stock markets using Zerodha Kite Connect API.

## Project Structure

```
tradingBot-main/
├── main.py                     # Main entry point for the trading bot
├── config/                     # Configuration files
│   └── config.py              # API keys and trading parameters
├── core/                       # Core trading components
│   └── brokerage_client.py    # Main brokerage client for Kite Connect
├── data_handlers/              # Data logging and management
│   ├── order_logger.py        # Order execution logging
│   └── price_logger.py        # Market data logging (renamed to MarketDataLogger)
├── strategies/                 # Trading strategies
│   └── macd_strategy.py       # MACD-based trading strategy
├── indicators/                 # Technical indicators
│   ├── __init__.py           # Package initialization
│   ├── base.py               # Base indicator class
│   └── macd.py               # MACD indicator implementation
├── utils/                     # Utility functions
├── backtesting/               # Backtesting engine
│   ├── backtest_engine.py     # Strategy backtesting functionality
│   └── report_dump/           # Backtesting reports and visualizations
├── data_dump/                 # Raw data storage
├── order_data/                # Order execution logs
└── stock_data/                # Historical and live price data
```

## Key Components

### Core Components
- **BrokerageClient**: Main interface to Kite Connect API for session management, data fetching, and order placement
- **MarketDataLogger**: Handles logging of live and historical price data
- **OrderLogger**: Logs all order executions and trading activities

### Trading Strategies
- **MACDStrategy**: Implements MACD-based buy/sell signals for backtesting

### Technical Indicators
- **MACDIndicator**: Real-time MACD calculation for live trading
- **BaseIndicator**: Abstract base class for all technical indicators

### Features
- Real-time market data streaming
- MACD-based trading signals
- Order placement (Market and Bracket orders)
- Comprehensive logging system
- Backtesting capabilities
- Historical data fetching

## Usage

1. Configure your API credentials in `config/config.py`
2. Run the main trading bot: `python main.py`
3. For backtesting: `python backtesting/backtest_engine.py`

## Configuration

Update `config/config.py` with your Kite Connect credentials:
- API_KEY
- API_SECRET  
- REQUEST_TOKEN
- EXCHANGES (NSE, BSE)
- INTRADAY_TARGET_STOCK

## Dependencies

- pandas
- numpy
- matplotlib
- kiteconnect

## Backtesting

The trading bot includes a comprehensive backtesting engine that allows you to test your strategies against historical data before deploying them in live trading.

### Features
- **Historical Data Analysis**: Test strategies against historical market data
- **Performance Metrics**: Generate detailed performance reports including:
  - Total returns and percentage gains
  - Win/loss ratios
  - Maximum drawdown
  - Sharpe ratio
  - Trade statistics
- **Visual Reports**: Automatic generation of charts and graphs showing:
  - Price movements with buy/sell signals
  - Portfolio value over time
  - Trade entry and exit points
- **Multi-timeframe Support**: Test strategies across different timeframes (1 minute, 10 minutes, 1 day)
- **CSV Export**: Export detailed trade logs and performance metrics

### Usage
1. Place your historical data in the `stock_data/` directory
2. Configure your strategy parameters in the respective strategy files
3. Run the backtesting engine: `python backtesting/backtest_engine.py`
4. View results in the `backtesting/report_dump/` directory

### Report Outputs
- **Signal Files**: CSV files containing all buy/sell signals with timestamps
- **Trade Logs**: Detailed records of all executed trades
- **Performance Charts**: PNG visualizations of strategy performance
- **Summary Reports**: Multi-timeframe performance summaries