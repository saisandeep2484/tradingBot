import pandas as pd  # type: ignore
import matplotlib.pyplot as plt  # type: ignore
import logging
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)


class MACDStrategy:
    """
    MACD (Moving Average Convergence Divergence) trading strategy implementation.
    
    Generates buy/sell signals based on MACD line crossovers with the signal line.
    Provides functionality for signal generation, visualization, and profit calculation.
    """
    
    # Constants
    DEFAULT_FAST_PERIOD = 12
    DEFAULT_SLOW_PERIOD = 26
    DEFAULT_SIGNAL_PERIOD = 9
    DEFAULT_QUANTITY = 100
    MIN_DATA_POINTS = 50  # Minimum data points required for reliable MACD calculation
    
    def __init__(self, df: pd.DataFrame, quantity: int = DEFAULT_QUANTITY, 
                 fast_period: int = DEFAULT_FAST_PERIOD, slow_period: int = DEFAULT_SLOW_PERIOD,
                 signal_period: int = DEFAULT_SIGNAL_PERIOD):
        """
        Initialize the MACD strategy.
        
        Args:
            df: DataFrame with OHLC data (must contain 'close' column)
            quantity: Number of shares/units to trade (default: 100)
            fast_period: Fast EMA period (default: 12)
            slow_period: Slow EMA period (default: 26)
            signal_period: Signal line EMA period (default: 9)
            
        Raises:
            ValueError: If invalid parameters or insufficient data provided
        """
        self._validate_inputs(df, quantity, fast_period, slow_period, signal_period)
        
        self.df = df.copy()
        self.quantity = quantity
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period
        self._signals_generated = False
        
        logger.info("Initialized MACD strategy with %d data points, quantity=%d, periods=(%d,%d,%d)",
                   len(self.df), quantity, fast_period, slow_period, signal_period)
    
    # Public methods (alphabetically ordered)
    
    def calculate_profit(self) -> Tuple[pd.DataFrame, float]:
        """
        Calculate profit from MACD trading signals.
        
        Returns:
            Tuple containing:
            - DataFrame with trade details (buy_price, sell_price, profit)
            - Total profit from all trades
            
        Raises:
            RuntimeError: If no valid trades can be calculated
        """
        try:
            if not self._signals_generated:
                self.generate_signals()
            
            trades = []
            position: Optional[Dict[str, float]] = None
            
            logger.debug("Calculating profits from MACD signals")
            
            for index, row in self.df.iterrows():
                if row["Buy_Signal"] and position is None:
                    # Enter long position
                    position = {"buy_price": row["close"], "buy_date": index}
                    logger.debug("Buy signal at %s: %.2f", index, row["close"])
                    
                elif row["Sell_Signal"] and position is not None:
                    # Exit long position
                    sell_price = row["close"]
                    profit = (sell_price - position["buy_price"]) * self.quantity
                    
                    trades.append({
                        "buy_date": position["buy_date"],
                        "sell_date": index,
                        "buy_price": position["buy_price"],
                        "sell_price": sell_price,
                        "profit": profit,
                        "return_pct": ((sell_price - position["buy_price"]) / position["buy_price"]) * 100
                    })
                    
                    logger.debug("Sell signal at %s: %.2f, Profit: %.2f", index, sell_price, profit)
                    position = None
            
            trades_df = pd.DataFrame(trades)
            total_profit = trades_df["profit"].sum() if not trades_df.empty else 0.0
            
            logger.info("Calculated %d trades with total profit: %.2f", len(trades_df), total_profit)
            return trades_df, total_profit
            
        except Exception as e:
            logger.error("Failed to calculate profit: %s", e)
            raise RuntimeError(f"Profit calculation failed: {e}") from e
    
    def generate_signals(self) -> pd.DataFrame:
        """
        Generate MACD trading signals.
        
        Calculates MACD line, signal line, and generates buy/sell signals based on crossovers.
        
        Returns:
            DataFrame with MACD indicators and trading signals
            
        Raises:
            RuntimeError: If signal generation fails
        """
        try:
            logger.debug("Generating MACD signals with periods (%d,%d,%d)", 
                        self.fast_period, self.slow_period, self.signal_period)
            
            # Calculate EMAs
            self.df["EMA_12"] = self.df["close"].ewm(span=self.fast_period, adjust=False).mean()
            self.df["EMA_26"] = self.df["close"].ewm(span=self.slow_period, adjust=False).mean()
            
            # Calculate MACD line and signal line
            self.df["MACD"] = self.df["EMA_12"] - self.df["EMA_26"]
            self.df["Signal"] = self.df["MACD"].ewm(span=self.signal_period, adjust=False).mean()
            self.df["MACD_Histogram"] = self.df["MACD"] - self.df["Signal"]
            
            # Generate trading signals
            self.df["Buy_Signal"] = (
                (self.df["MACD"] > self.df["Signal"]) & 
                (self.df["MACD"].shift(1) <= self.df["Signal"].shift(1))
            )
            self.df["Sell_Signal"] = (
                (self.df["MACD"] < self.df["Signal"]) & 
                (self.df["MACD"].shift(1) >= self.df["Signal"].shift(1))
            )
            
            # Count signals
            buy_signals = self.df["Buy_Signal"].sum()
            sell_signals = self.df["Sell_Signal"].sum()
            
            logger.info("Generated %d buy signals and %d sell signals", buy_signals, sell_signals)
            self._signals_generated = True
            
            return self.df
            
        except Exception as e:
            logger.error("Failed to generate MACD signals: %s", e)
            raise RuntimeError(f"Signal generation failed: {e}") from e
    
    def get_current_signal(self) -> Optional[str]:
        """
        Get the most recent trading signal.
        
        Returns:
            'BUY', 'SELL', or None if no recent signal
        """
        if not self._signals_generated:
            self.generate_signals()
        
        if self.df.empty:
            return None
            
        latest_row = self.df.iloc[-1]
        
        if latest_row["Buy_Signal"]:
            return "BUY"
        elif latest_row["Sell_Signal"]:
            return "SELL"
        else:
            return None
    
    def get_strategy_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive strategy statistics.
        
        Returns:
            Dictionary containing strategy performance metrics
        """
        trades_df, total_profit = self.calculate_profit()
        
        if trades_df.empty:
            return {
                "total_trades": 0,
                "total_profit": 0.0,
                "win_rate": 0.0,
                "avg_profit_per_trade": 0.0,
                "max_profit": 0.0,
                "max_loss": 0.0
            }
        
        winning_trades = trades_df[trades_df["profit"] > 0]
        losing_trades = trades_df[trades_df["profit"] < 0]
        
        stats = {
            "total_trades": len(trades_df),
            "total_profit": total_profit,
            "win_rate": (len(winning_trades) / len(trades_df)) * 100,
            "avg_profit_per_trade": trades_df["profit"].mean(),
            "avg_return_pct": trades_df["return_pct"].mean(),
            "max_profit": trades_df["profit"].max(),
            "max_loss": trades_df["profit"].min(),
            "winning_trades": len(winning_trades),
            "losing_trades": len(losing_trades)
        }
        
        logger.info("Strategy stats: %d trades, %.2f%% win rate, %.2f total profit", 
                   stats["total_trades"], stats["win_rate"], stats["total_profit"])
        
        return stats
    
    def plot_signals(self, save_path: Optional[str] = None, show_plot: bool = True) -> None:
        """
        Plot MACD strategy signals and indicators.
        
        Args:
            save_path: Optional path to save the plot
            show_plot: Whether to display the plot (default: True)
            
        Raises:
            RuntimeError: If plotting fails
        """
        try:
            if not self._signals_generated:
                self.generate_signals()
            
            logger.debug("Creating MACD strategy plot")
            
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)
            
            # Price chart with buy/sell signals
            ax1.plot(self.df.index, self.df["close"], label="Close Price", color='black', linewidth=1)
            
            # Buy signals
            buy_points = self.df[self.df["Buy_Signal"]]
            if not buy_points.empty:
                ax1.scatter(buy_points.index, buy_points["close"],
                           label=f"Buy Signals ({len(buy_points)})", marker="^", 
                           color="green", s=100, zorder=5)
            
            # Sell signals
            sell_points = self.df[self.df["Sell_Signal"]]
            if not sell_points.empty:
                ax1.scatter(sell_points.index, sell_points["close"],
                           label=f"Sell Signals ({len(sell_points)})", marker="v", 
                           color="red", s=100, zorder=5)
            
            ax1.set_title(f"MACD Strategy - Price with Trading Signals (Quantity: {self.quantity})")
            ax1.set_ylabel("Price")
            ax1.legend()
            ax1.grid(True, alpha=0.3)
            
            # MACD indicator
            ax2.plot(self.df.index, self.df["MACD"], label="MACD Line", color='blue', linewidth=1)
            ax2.plot(self.df.index, self.df["Signal"], label="Signal Line", color='orange', linewidth=1)
            
            # MACD histogram
            colors = ['green' if x > 0 else 'red' for x in self.df["MACD_Histogram"]]
            ax2.bar(self.df.index, self.df["MACD_Histogram"], label="MACD Histogram",
                   color=colors, alpha=0.6, width=0.8)
            
            ax2.axhline(y=0, color='black', linestyle='-', alpha=0.3)
            ax2.set_title(f"MACD Indicator (Periods: {self.fast_period}, {self.slow_period}, {self.signal_period})")
            ax2.set_xlabel("Date")
            ax2.set_ylabel("MACD Value")
            ax2.legend()
            ax2.grid(True, alpha=0.3)
            
            plt.tight_layout()
            
            if save_path:
                plt.savefig(save_path, dpi=300, bbox_inches='tight')
                logger.info("Plot saved to: %s", save_path)
            
            if show_plot:
                plt.show()
            else:
                plt.close()
                
        except Exception as e:
            logger.error("Failed to create plot: %s", e)
            raise RuntimeError(f"Plotting failed: {e}") from e
    
    def save_signals_to_csv(self, file_path: str) -> bool:
        """
        Save generated signals and indicators to CSV file.
        
        Args:
            file_path: Path to save the CSV file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not self._signals_generated:
                self.generate_signals()
            
            # Select relevant columns for export
            export_columns = [
                "close", "EMA_12", "EMA_26", "MACD", "Signal", "MACD_Histogram",
                "Buy_Signal", "Sell_Signal"
            ]
            
            export_df = self.df[export_columns].copy()
            export_df.to_csv(file_path, index=True)
            
            logger.info("Saved MACD signals to: %s", file_path)
            return True
            
        except Exception as e:
            logger.error("Failed to save signals to CSV: %s", e)
            return False
    
    # Private methods (alphabetically ordered)
    
    def _validate_inputs(self, df: pd.DataFrame, quantity: int, fast_period: int, 
                        slow_period: int, signal_period: int) -> None:
        """
        Validate input parameters for MACD strategy.
        
        Args:
            df: Input DataFrame
            quantity: Trading quantity
            fast_period: Fast EMA period
            slow_period: Slow EMA period
            signal_period: Signal line period
            
        Raises:
            ValueError: If any parameter is invalid
        """
        if df is None or df.empty:
            raise ValueError("DataFrame cannot be None or empty")
        
        if "close" not in df.columns:
            raise ValueError("DataFrame must contain 'close' column")
        
        if len(df) < self.MIN_DATA_POINTS:
            raise ValueError(f"Insufficient data: need at least {self.MIN_DATA_POINTS} points, got {len(df)}")
        
        if quantity <= 0:
            raise ValueError(f"Quantity must be positive, got: {quantity}")
        
        if fast_period <= 0 or slow_period <= 0 or signal_period <= 0:
            raise ValueError("All periods must be positive integers")
        
        if fast_period >= slow_period:
            raise ValueError(f"Fast period ({fast_period}) must be less than slow period ({slow_period})")
        
        if df["close"].isna().any():
            raise ValueError("Close prices contain NaN values")
        
        if (df["close"] <= 0).any():
            raise ValueError("Close prices must be positive")
        
        logger.debug("Input validation passed for MACD strategy")